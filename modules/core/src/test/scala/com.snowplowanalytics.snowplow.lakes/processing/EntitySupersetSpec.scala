/*
 * Copyright (c) 2014-present Snowplow Analytics Ltd. All rights reserved.
 *
 * This software is made available by Snowplow Analytics, Ltd.,
 * under the terms of the Snowplow Limited Use License Agreement, Version 1.1
 * located at https://docs.snowplow.io/limited-use-license-1.1
 * BY INSTALLING, DOWNLOADING, ACCESSING, USING OR DISTRIBUTING ANY PORTION
 * OF THE SOFTWARE, YOU AGREE TO THE TERMS OF SUCH LICENSE AGREEMENT.
 */

package com.snowplowanalytics.snowplow.lakes.processing

import cats.effect.{IO, Ref}
import cats.implicits._
import cats.effect.testing.specs2.CatsEffect
import org.specs2.Specification
import org.specs2.matcher.MatchResult

import com.snowplowanalytics.iglu.client.Resolver
import com.snowplowanalytics.iglu.client.resolver.registries.{JavaNetRegistryLookup, RegistryLookup}
import com.snowplowanalytics.iglu.core.SchemaCriterion
import com.snowplowanalytics.snowplow.badrows.FailureDetails
import com.snowplowanalytics.snowplow.loaders.transform.{NonAtomicFields, SchemaSubVersion, TabledEntity}
import com.snowplowanalytics.snowplow.loaders.transform.NonAtomicFields.{ColumnFailure, ResolveTypesException}

class EntitySupersetSpec extends Specification with CatsEffect {
  import EntitySupersetSpec._

  def is = s2"""
  EntitySuperset.resolve should:
    Pad the batch to the superset, in column-name order, and report only the batch's own columns $e1
    Give batches with different entity sets the identical Spark schema once the superset has settled $e2
    Report a recovery column as the batch's own only when the batch has that sub-version $e3
    Not call resolveTypes for a batch whose entities and versions are all in the superset $e4
    Resolve only the extended family, with the batch's own versions, for a new version of a known entity $e5
    Keep the known type and fail only the new versions when a new version of a known entity fails $e6
    Fail only the new versions when the registry is down for a known entity $e7
    Fail a newcomer exactly as resolving the batch alone does, and not store it $e8
    Detect clashing entities that are in different batches $e9
    Never hold more entries than its capacity $e10
    When full, replace the coldest entry not seen in the current or previous window, and report the replacement $e11
    When full of recently seen entries, give the newcomer the fallback and leave the superset unchanged $e12
    Give a batch without the fallback newcomer the plain superset's schema $e13
    Detect a clash that arrives through the fallback $e14
    Treat an entry seen in the previous window as in use, and one seen two windows ago as cold $e15
    Not let a newcomer displace a cold entry that shares its column name $e16
    Not record a version the type doesn't cover, and resolve the family again for the next batch carrying it $e17
    Narrow failures from the versions left after skipSchemas $e20
    Narrow failures by every version the stored type covers $e21
    Never treat a skipped version as new $e22
    Not report an entity that lost an admission race as not stored $e23
    Never report a version that yields no column as a failure, alone or with one that does $e24
    Resolve a lower revision than the stored one as the batch alone does, and leave the entry unchanged $e25
    Use a type that is neither wider nor narrower than the entry for that batch only $e26
  """

  def e1 =
    for {
      superset <- settled(Map(goodSchema -> v(0, 0)))
      result <- resolveForReal(superset, Map(goodContext -> v(0, 0)))
      stored <- superset.get
    } yield result must beRight.like { case r =>
      List[MatchResult[Any]](
        r.result.fields.map(_.mergedField.name) must beEqualTo(Vector(goodContextColumn, goodSchemaColumn)),
        r.columnNames must beEqualTo(Set(goodContextColumn)),
        r.size must beEqualTo(2),
        stored.view.mapValues(_.versions).toMap must beEqualTo(Map(goodSchema -> v(0, 0), goodContext -> v(0, 0)))
      ).reduce(_ and _).toResult
    }

  def e2 = {
    val batches = List(
      Map(goodContext -> v(0, 0)),
      Map(badEvolution -> v(0, 0), goodSchema -> v(0, 0)),
      Map(goodSchema -> v(0, 0), badEvolution -> v(0, 1)),
      Map.empty[TabledEntity, Set[SchemaSubVersion]]
    )
    for {
      superset <- settled(Map(goodSchema -> v(0, 0), goodContext -> v(0, 0), badEvolution -> (v(0, 0) ++ v(0, 1))))
      results <- batches.traverse(resolveForReal(superset, _))
    } yield {
      val schemas = results.collect { case Right(r) => SparkSchema.forBatch(r.result.fields, respectIgluNullability = true) }
      (schemas must haveSize(batches.size)) and (schemas.distinct must haveSize(1))
    }
  }

  def e3 =
    for {
      superset <- settled(Map(badEvolution -> (v(0, 0) ++ v(0, 1))))
      without <- resolveForReal(superset, Map(badEvolution -> v(0, 0)))
      withIt <- resolveForReal(superset, Map(badEvolution -> v(0, 1)))
    } yield {
      def columns(r: Either[ResolveTypesException, EntitySuperset.Resolved]) =
        r.toOption.toList.flatMap(_.result.fields.flatMap(tte => tte.mergedField.name :: tte.recoveries.map(_._2.name)))
      val recovered = columns(without).filter(_.startsWith(s"${badEvolutionColumn}_recovered_1_0_1_"))

      List[MatchResult[Any]](
        recovered must haveSize(1),
        columns(withIt) must beEqualTo(columns(without)),
        without.toOption.map(_.columnNames) must beEqualTo(Some(Set[String](badEvolutionColumn))),
        withIt.toOption.map(_.columnNames) must beEqualTo(Some(Set[String](badEvolutionColumn) ++ recovered))
      ).reduce(_ and _)
    }

  def e4 =
    for {
      superset <- settled(Map(goodSchema -> (v(0, 0) ++ v(0, 1)), goodContext -> v(0, 0)))
      calls <- Ref[IO].of(List.empty[EntitySuperset.Entities])
      results <- List(Map(goodSchema -> v(0, 1)), Map(goodContext -> v(0, 0), goodSchema -> v(0, 0)), Map.empty: EntitySuperset.Entities)
                   .traverse(EntitySuperset.resolve[IO](counting(calls, realResolveTypes), Nil, superset, 500, 1, _))
      made <- calls.get
    } yield (made must beEmpty) and (results.map(_.map(_.result.fields.size)) must beEqualTo(List.fill(3)(Right(2))))

  def e5 =
    for {
      superset <- settled(Map(goodSchema -> v(0, 0), goodContext -> v(0, 0)))
      calls <- Ref[IO].of(List.empty[EntitySuperset.Entities])
      result <-
        EntitySuperset
          .resolve[IO](counting(calls, realResolveTypes), Nil, superset, 500, 1, Map(goodSchema -> v(0, 1), goodContext -> v(0, 0)))
      made <- calls.get
      stored <- superset.get
    } yield List[MatchResult[Any]](
      made must beEqualTo(List(Map(goodSchema -> v(0, 1)))),
      result.map(_.result.igluFailures) must beEqualTo(Right(List.empty[ColumnFailure])),
      stored.get(goodSchema).map(_.versions) must beEqualTo(Some(v(0, 0) ++ v(0, 1))),
      stored.get(goodSchema).map(_.typed.mergedVersions) must beEqualTo(Some(v(0, 0) ++ v(0, 1)))
    ).reduce(_ and _)

  def e6 =
    for {
      superset <- settled(Map(goodSchema -> v(0, 0)))
      result <- resolveForReal(superset, Map(goodSchema -> (v(0, 0) ++ v(0, 9))))
      stored <- superset.get
    } yield List[MatchResult[Any]](
      result.map(_.result.fields.map(_.mergedVersions)) must beEqualTo(Right(Vector(v(0, 0)))),
      result.map(_.result.igluFailures.map(f => (f.tabledEntity, f.versionsInBatch))) must beEqualTo(Right(List((goodSchema, v(0, 9))))),
      result.map(_.columnNames) must beEqualTo(Right(Set(goodSchemaColumn))),
      stored.get(goodSchema).map(_.versions) must beEqualTo(Some(v(0, 0)))
    ).reduce(_ and _)

  def e7 =
    for {
      superset <- settled(Map(goodSchema -> v(0, 0)))
      result <- EntitySuperset.resolve[IO](alwaysFails, Nil, superset, 500, 1, Map(goodSchema -> (v(0, 0) ++ v(0, 1))))
      stored <- superset.get
    } yield List[MatchResult[Any]](
      result.map(_.result.fields.map(_.tabledEntity)) must beEqualTo(Right(Vector(goodSchema))),
      result.map(_.result.igluFailures.map(f => (f.tabledEntity, f.versionsInBatch))) must beEqualTo(Right(List((goodSchema, v(0, 1))))),
      stored.get(goodSchema).map(_.versions) must beEqualTo(Some(v(0, 0)))
    ).reduce(_ and _)

  def e8 = {
    val batch = Map(doesNotExist -> v(0, 0))
    for {
      superset <- settled(Map(goodSchema -> v(0, 0)))
      result <- resolveForReal(superset, batch)
      today <- NonAtomicFields.resolveTypes[IO](resolver, batch, Nil)
      stored <- superset.get
    } yield List[MatchResult[Any]](
      result.map(r => failures(r.result)) must beEqualTo(today.map(failures)),
      result.map(r => failures(r.result)) must beEqualTo(Right(List((doesNotExist, v(0, 0))))),
      result.map(_.result.fields.map(_.tabledEntity)) must beEqualTo(Right(Vector(goodSchema))),
      stored.keySet must beEqualTo(Set(goodSchema))
    ).reduce(_ and _)
  }

  def e9 =
    for {
      superset <- settled(Map(clashingA -> v(0, 0)))
      result <- resolveForReal(superset, Map(clashingB -> v(0, 0)))
      stored <- superset.get
    } yield (result must beLeft(ResolveTypesException.ClashingFieldNames(Set(Set(clashingA, clashingB))))) and
      (stored.keySet must beEqualTo(Set(clashingA)))

  def e10 =
    for {
      superset <- Ref[IO].of(Map.empty: EntitySuperset.Superset)
      sizes <- List(goodSchema, goodContext, badEvolution, nested, nullableEvol).zipWithIndex.traverse { case (te, i) =>
                 resolveForReal(superset, Map(te -> v(0, 0)), capacity = 2, window = i * 3 + 1) *> superset.get.map(_.size)
               }
    } yield sizes must beEqualTo(List(1, 2, 2, 2, 2))

  def e11 =
    for {
      superset <- Ref[IO].of(Map.empty: EntitySuperset.Superset)
      _ <- resolveForReal(superset, Map(goodSchema -> v(0, 0)), capacity = 3, window = 1)
      _ <- resolveForReal(superset, Map(goodContext -> v(0, 0)), capacity = 3, window = 2)
      _ <- resolveForReal(superset, Map(badEvolution -> v(0, 0)), capacity = 3, window = 3)
      // Window 4: goodSchema (window 1) and goodContext (window 2) are both cold, and goodSchema is
      // the coldest. badEvolution was seen in the previous window, so it is not cold.
      result <- resolveForReal(superset, Map(nested -> v(0, 0)), capacity = 3, window = 4)
      stored <- superset.get
    } yield List[MatchResult[Any]](
      stored.keySet must beEqualTo(Set(goodContext, badEvolution, nested)),
      result.map(_.notStored) must beEqualTo(Right(Set.empty[TabledEntity])),
      result.map(_.displaced) must beEqualTo(Right(List(EntitySuperset.Displacement(goodSchema, 1, nested))))
    ).reduce(_ and _)

  def e12 =
    for {
      superset <- Ref[IO].of(Map.empty: EntitySuperset.Superset)
      _ <- resolveForReal(superset, Map(goodSchema -> v(0, 0)), capacity = 2, window = 1)
      _ <- resolveForReal(superset, Map(goodContext -> v(0, 0)), capacity = 2, window = 2)
      before <- superset.get
      result <- resolveForReal(superset, Map(nested -> v(0, 0)), capacity = 2, window = 2)
      after <- superset.get
    } yield List[MatchResult[Any]](
      after must beEqualTo(before),
      result.map(_.notStored) must beEqualTo(Right(Set(nested))),
      result.map(_.displaced) must beEqualTo(Right(Nil)),
      result.map(_.result.fields.map(_.mergedField.name)) must beEqualTo(Right(Vector(goodContextColumn, goodSchemaColumn, nestedColumn))),
      result.map(_.columnNames) must beEqualTo(Right(Set(nestedColumn))),
      result.map(_.size) must beEqualTo(Right(2))
    ).reduce(_ and _)

  def e13 =
    for {
      superset <- Ref[IO].of(Map.empty: EntitySuperset.Superset)
      _ <- resolveForReal(superset, Map(goodSchema -> v(0, 0), goodContext -> v(0, 0)), capacity = 2, window = 1)
      plain <- resolveForReal(superset, Map(goodSchema -> v(0, 0)), capacity = 2, window = 1)
      _ <- resolveForReal(superset, Map(nested -> v(0, 0)), capacity = 2, window = 1)
      later <- resolveForReal(superset, Map(goodSchema -> v(0, 0)), capacity = 2, window = 1)
    } yield {
      def schema(r: Either[ResolveTypesException, EntitySuperset.Resolved]) =
        r.map(x => SparkSchema.forBatch(x.result.fields, respectIgluNullability = true))
      (schema(later) must beEqualTo(schema(plain))) and (later.map(_.notStored) must beEqualTo(Right(Set.empty[TabledEntity])))
    }

  def e15 =
    for {
      superset <- Ref[IO].of(Map.empty: EntitySuperset.Superset)
      _ <- resolveForReal(superset, Map(goodSchema -> v(0, 0)), capacity = 1, window = 1)
      inPrevious <- resolveForReal(superset, Map(nested -> v(0, 0)), capacity = 1, window = 2)
      afterPrevious <- superset.get
      twoAgo <- resolveForReal(superset, Map(nested -> v(0, 0)), capacity = 1, window = 3)
      afterTwoAgo <- superset.get
    } yield List[MatchResult[Any]](
      inPrevious.map(_.notStored) must beEqualTo(Right(Set(nested))),
      afterPrevious.keySet must beEqualTo(Set(goodSchema)),
      twoAgo.map(_.displaced) must beEqualTo(Right(List(EntitySuperset.Displacement(goodSchema, 1, nested)))),
      afterTwoAgo.keySet must beEqualTo(Set(nested))
    ).reduce(_ and _)

  def e16 =
    for {
      superset <- Ref[IO].of(Map.empty: EntitySuperset.Superset)
      _ <- resolveForReal(superset, Map(clashingA -> v(0, 0)), capacity = 1, window = 1)
      result <- resolveForReal(superset, Map(clashingB -> v(0, 0)), capacity = 1, window = 5)
      stored <- superset.get
    } yield (result must beLeft(ResolveTypesException.ClashingFieldNames(Set(Set(clashingA, clashingB))))) and
      (stored.keySet must beEqualTo(Set(clashingA)))

  def e17 = {
    def check(
      te: TabledEntity,
      stored: Set[SchemaSubVersion],
      uncovered: Set[SchemaSubVersion]
    ) =
      for {
        superset <- settled(Map(te -> stored))
        _ <- resolveForReal(superset, Map(te -> uncovered))
        after <- superset.get
        calls <- Ref[IO].of(List.empty[EntitySuperset.Entities])
        _ <- EntitySuperset.resolve[IO](counting(calls, realResolveTypes), Nil, superset, 500, 1, Map(te -> uncovered))
        made <- calls.get
      } yield List[MatchResult[Any]](
        after.get(te).map(_.versions.intersect(uncovered)) must beEqualTo(Some(Set.empty[SchemaSubVersion])),
        made.map(_.get(te).map(uncovered.subsetOf)) must beEqualTo(List(Some(true)))
      ).reduce(_ and _)

    // A version yielding no column, and a lower revision the family's lookup passes over.
    (check(emptyFirst, v(0, 1), v(0, 0)), check(twoRevisions, v(1, 0), v(0, 2))).mapN(_ and _)
  }

  def e20 = {
    val batch = Map(goodSchema -> (v(0, 1) ++ v(0, 9)))
    for {
      superset <- Ref[IO].of(Map.empty: EntitySuperset.Superset)
      result <- EntitySuperset.resolve[IO](skipping, skipGoodSchema701, superset, 500, 1, batch)
      alone <- NonAtomicFields.resolveTypes[IO](resolver, batch, skipGoodSchema701)
    } yield List[MatchResult[Any]](
      result.map(r => failures(r.result)) must beEqualTo(Right(List((goodSchema, v(0, 9))))),
      result.map(r => failures(r.result)) must beEqualTo(alone.map(failures))
    ).reduce(_ and _)
  }

  def e21 =
    for {
      superset <- settled(Map(goodSchema -> v(0, 1)))
      result <- resolveForReal(superset, Map(goodSchema -> (v(0, 0) ++ v(0, 9))))
      stored <- superset.get
    } yield List[MatchResult[Any]](
      stored.get(goodSchema).map(_.versions) must beEqualTo(Some(v(0, 0) ++ v(0, 1))),
      result.map(r => failures(r.result)) must beEqualTo(Right(List((goodSchema, v(0, 9)))))
    ).reduce(_ and _)

  def e22 =
    for {
      superset <- settled(Map(goodSchema -> v(0, 0)))
      calls <- Ref[IO].of(List.empty[EntitySuperset.Entities])
      result <- EntitySuperset.resolve[IO](counting(calls, alwaysFails), skipGoodSchema701, superset, 500, 1, Map(goodSchema -> v(0, 1)))
      made <- calls.get
    } yield (made must beEmpty) and (result.map(_.result.igluFailures) must beEqualTo(Right(List.empty[ColumnFailure])))

  def e23 =
    for {
      superset <- Ref[IO].of(Map.empty: EntitySuperset.Superset)
      // Another batch's admission lands while this batch resolves, storing an entry whose versions
      // are neither a subset nor a superset of this batch's result.
      racing = (entities: EntitySuperset.Entities) =>
                 realResolveTypes(Map(goodSchema -> v(0, 0))).flatMap { other =>
                   val typed = other.toOption.get.fields.head
                   superset.set(Map(goodSchema -> EntitySuperset.Entry(v(0, 0) ++ v(0, 7), typed, 1)))
                 } *> realResolveTypes(entities)
      result <- EntitySuperset.resolve[IO](racing, Nil, superset, 500, 1, Map(goodSchema -> v(0, 1)))
      stored <- superset.get
    } yield List[MatchResult[Any]](
      result.map(_.notStored) must beEqualTo(Right(Set.empty[TabledEntity])),
      result.map(_.result.fields.map(_.mergedVersions)) must beEqualTo(Right(Vector(v(0, 0) ++ v(0, 1)))),
      stored.get(goodSchema).map(_.versions) must beEqualTo(Some(v(0, 0) ++ v(0, 7)))
    ).reduce(_ and _)

  def e24 =
    for {
      superset <- settled(Map(emptyFirst -> v(0, 1)))
      after <- resolveForReal(superset, Map(emptyFirst -> v(0, 0)))
      fresh <- Ref[IO].of(Map.empty: EntitySuperset.Superset)
      together <- resolveForReal(fresh, Map(emptyFirst -> (v(0, 0) ++ v(0, 1))))
    } yield List[MatchResult[Any]](
      after.map(_.result.igluFailures) must beEqualTo(Right(List.empty[ColumnFailure])),
      after.map(_.columnNames) must beEqualTo(Right(Set(emptyFirstColumn))),
      together.map(_.result.igluFailures) must beEqualTo(Right(List.empty[ColumnFailure])),
      together.map(_.columnNames) must beEqualTo(Right(Set(emptyFirstColumn)))
    ).reduce(_ and _)

  def e25 = {
    val batch = Map(twoRevisions -> v(0, 2))
    for {
      superset <- settled(Map(twoRevisions -> v(1, 0)))
      before <- superset.get
      result <- resolveForReal(superset, batch)
      alone <- NonAtomicFields.resolveTypes[IO](resolver, batch, Nil)
      after <- superset.get
    } yield List[MatchResult[Any]](
      result.map(r => failures(r.result)) must beEqualTo(alone.map(failures)),
      result.map(r => failures(r.result)) must beEqualTo(Right(List((twoRevisions, v(0, 2))))),
      after.view.mapValues(_.versions).toMap must beEqualTo(before.view.mapValues(_.versions).toMap)
    ).reduce(_ and _)
  }

  def e26 =
    for {
      superset <- settled(Map(goodSchema -> (v(0, 0) ++ v(0, 1)), goodContext -> v(0, 0)))
      before <- superset.get
      plain <- resolveForReal(superset, Map(goodContext -> v(0, 0)))
      // A type covering 7-0-2, which the entry lacks, but not 7-0-1, which the entry has.
      neither = (_: EntitySuperset.Entities) =>
                  realResolveTypes(Map(goodSchema -> v(0, 0))).map(_.map { r =>
                    r.copy(fields = r.fields.map(_.copy(mergedVersions = v(0, 0) ++ v(0, 2))))
                  })
      result <- EntitySuperset.resolve[IO](neither, Nil, superset, 500, 1, Map(goodSchema -> v(0, 2)))
      after <- superset.get
      later <- resolveForReal(superset, Map(goodContext -> v(0, 0)))
    } yield {
      def schema(r: Either[ResolveTypesException, EntitySuperset.Resolved]) =
        r.map(x => SparkSchema.forBatch(x.result.fields, respectIgluNullability = true))
      List[MatchResult[Any]](
        result.map(_.result.fields.find(_.tabledEntity == goodSchema).map(_.mergedVersions)) must beEqualTo(
          Right(Some(v(0, 0) ++ v(0, 2)))
        ),
        result.map(_.notStored) must beEqualTo(Right(Set.empty[TabledEntity])),
        after must beEqualTo(before),
        schema(later) must beEqualTo(schema(plain))
      ).reduce(_ and _)
    }

  def e14 =
    for {
      superset <- Ref[IO].of(Map.empty: EntitySuperset.Superset)
      _ <- resolveForReal(superset, Map(clashingA -> v(0, 0)), capacity = 1, window = 1)
      result <- resolveForReal(superset, Map(clashingB -> v(0, 0)), capacity = 1, window = 1)
      stored <- superset.get
    } yield (result must beLeft(ResolveTypesException.ClashingFieldNames(Set(Set(clashingA, clashingB))))) and
      (stored.keySet must beEqualTo(Set(clashingA)))
}

object EntitySupersetSpec {

  private implicit val lookup: RegistryLookup[IO] = JavaNetRegistryLookup.ioLookupInstance[IO]

  private val resolver = Resolver[IO](Nil, None)

  private val realResolveTypes: EntitySuperset.ResolveTypes[IO] = NonAtomicFields.resolveTypes[IO](resolver, _, Nil)

  private def resolveForReal(
    superset: Ref[IO, EntitySuperset.Superset],
    batch: EntitySuperset.Entities,
    capacity: Int = 500,
    window: Int   = 1
  ): IO[Either[ResolveTypesException, EntitySuperset.Resolved]] =
    EntitySuperset.resolve[IO](realResolveTypes, Nil, superset, capacity, window, batch)

  /** A superset that holds exactly these entities, all seen in window 1. */
  private def settled(entities: EntitySuperset.Entities): IO[Ref[IO, EntitySuperset.Superset]] =
    for {
      superset <- Ref[IO].of(Map.empty: EntitySuperset.Superset)
      result <- resolveForReal(superset, entities)
      _ <- IO.fromEither(result)
    } yield superset

  private def counting(
    calls: Ref[IO, List[EntitySuperset.Entities]],
    underlying: EntitySuperset.ResolveTypes[IO]
  ): EntitySuperset.ResolveTypes[IO] =
    entities => calls.update(_ :+ entities) *> underlying(entities)

  /** Compared without the failure details, which carry the time of each lookup attempt. */
  private def failures(r: NonAtomicFields.Result): List[(TabledEntity, Set[SchemaSubVersion])] =
    r.igluFailures.map(f => (f.tabledEntity, f.versionsInBatch))

  /** Resolves with `skipGoodSchema701`, as `Processing` passes `skipSchemas` to both. */
  private val skipping: EntitySuperset.ResolveTypes[IO] = NonAtomicFields.resolveTypes[IO](resolver, _, skipGoodSchema701)

  /** A registry outage: every family fails, whichever versions are asked for. */
  private val alwaysFails: EntitySuperset.ResolveTypes[IO] = entities =>
    IO.pure(
      Right(
        NonAtomicFields.Result(
          Vector.empty,
          entities.toList.map { case (te, versions) =>
            ColumnFailure(te, versions, FailureDetails.LoaderIgluError.InvalidSchema(TabledEntity.toSchemaKey(te, versions.max), "boom"))
          }
        )
      )
    )

  private def v(revision: Int, addition: Int): Set[SchemaSubVersion] = Set((revision, addition))

  private val goodSchema   = TabledEntity(TabledEntity.UnstructEvent, "myvendor", "goodschema", 7)
  private val goodContext  = TabledEntity(TabledEntity.Context, "myvendor", "goodcontext", 1)
  private val badEvolution = TabledEntity(TabledEntity.UnstructEvent, "myvendor", "badevolution", 1)
  private val nested       = TabledEntity(TabledEntity.UnstructEvent, "myvendor", "nestedschema", 1)
  private val nullableEvol = TabledEntity(TabledEntity.UnstructEvent, "myvendor", "nullableevol", 1)
  private val doesNotExist = TabledEntity(TabledEntity.UnstructEvent, "myvendor", "doesnotexist", 1)
  private val clashingA    = TabledEntity(TabledEntity.Context, "clashing.a", "b_c", 1)
  private val clashingB    = TabledEntity(TabledEntity.Context, "clashing", "a_b_c", 1)
  private val twoRevisions = TabledEntity(TabledEntity.UnstructEvent, "myvendor", "tworevisions", 1)
  private val emptyFirst   = TabledEntity(TabledEntity.UnstructEvent, "myvendor", "emptyfirst", 1)

  private val skipGoodSchema701 = SchemaCriterion.parse("iglu:myvendor/goodschema/jsonschema/7-0-1").toList

  private val goodSchemaColumn   = "unstruct_event_myvendor_goodschema_7"
  private val goodContextColumn  = "contexts_myvendor_goodcontext_1"
  private val badEvolutionColumn = "unstruct_event_myvendor_badevolution_1"
  private val nestedColumn       = "unstruct_event_myvendor_nestedschema_1"
  private val emptyFirstColumn   = "unstruct_event_myvendor_emptyfirst_1"
}
