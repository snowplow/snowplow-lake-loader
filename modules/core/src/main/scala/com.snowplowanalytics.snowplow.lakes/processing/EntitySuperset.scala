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

import cats.effect.{Ref, Sync}
import cats.implicits._

import com.snowplowanalytics.iglu.core.SchemaCriterion
import com.snowplowanalytics.snowplow.loaders.transform.{NonAtomicFields, SchemaSubVersion, TabledEntity, TypedTabledEntity}
import com.snowplowanalytics.snowplow.loaders.transform.NonAtomicFields.ResolveTypesException

/**
 * Pads every batch to a bounded, process-wide superset of entities, so that every batch gets the
 * same schema once the superset stops changing, and Spark can reuse the code it generated for it.
 *
 * What you need to know:
 *
 *   - '''Superset''': the entities every batch is padded to, each with its resolved type, the
 *     versions that type covers, and the last window a batch carried it in.
 *   - '''Entry''': one entity in the superset.
 *   - '''Newcomer''': an entity a batch carries that is not in the superset.
 *   - '''Extended entity''': an entity in the superset that a batch carries with versions its entry
 *     does not record. Its family is resolved again with the versions the batch carries.
 *   - '''Cold''': an entry seen in neither the current window nor the previous one.
 *   - '''Displacement''': a newcomer replacing the coldest entry when the superset is full.
 *   - '''Fallback''': what happens to a newcomer when the superset is full and no entry is cold. It
 *     is not stored, and only its own batch's schema carries it, as the superset plus its fields.
 *   - '''Admission''': adding newcomers, replacing extended entries and marking entries as seen.
 *
 * The previous window counts as recent because at the start of a window nothing has been seen in it
 * yet, so the current window alone would make every entry look cold.
 *
 * Admission is one `Ref.modify`, after resolution and outside it. A batch that loses a race with
 * another batch's admission, so that its resolved type no longer fits the stored entry, takes the
 * fallback for that entity.
 *
 * An entry records the versions its type covers: its merged versions and its recoveries. A batch
 * carrying a version its entry does not record looks the family up with only the versions the batch
 * carries, so the lookup does not depend on what earlier batches stored. The result is reconciled
 * with the entry:
 *
 *   - a type covering more versions than the entry replaces it
 *   - a type covering fewer leaves the entry unchanged, and the batch uses the entry's type
 *   - a type that is neither is used for that batch only, as the fallback, and the entry is
 *     unchanged
 *
 * A version requested can be missing from the type, because its schema yields no field, or because
 * the lookup walks up to the highest version requested and passes over a lower-revision version it
 * cannot find. The entry does not record it, so every batch that carries it resolves the family
 * again. Failures are narrowed to the versions the entry does not record, and a version that failed
 * is never recorded. Versions matching `skipSchemas` are dropped first, as `resolveTypes` does, so
 * they are never new.
 *
 * Two entities whose columns share a name are rejected with `ClashingFieldNames`, a readable error
 * where the table library's would be cryptic. The check runs against the superset as it was before
 * the admission, so it covers entries the same admission displaces. It does not cover entries
 * displaced by an earlier admission, or newcomers that took the fallback in an earlier batch,
 * though batches already staged in the window may carry their columns.
 *
 * A schema that yields no column is an object with no properties and `additionalProperties: false`,
 * which is common for unstruct events. A family where no version yields a column has no type to
 * store. It is never in the superset, so every batch that carries it resolves it again.
 */
private[processing] object EntitySuperset {

  type Entities = Map[TabledEntity, Set[SchemaSubVersion]]

  type ResolveTypes[F[_]] = Entities => F[Either[ResolveTypesException, NonAtomicFields.Result]]

  /** `lastSeen` is a `WindowState.id`. */
  final case class Entry(
    versions: Set[SchemaSubVersion],
    typed: TypedTabledEntity,
    lastSeen: Int
  )

  /**
   * Each batch's entities are normally a subset of it, which is why every batch can be padded to
   * it. A batch only carries entities outside it when it is full.
   */
  type Superset = Map[TabledEntity, Entry]

  final case class Displacement(
    removed: TabledEntity,
    lastSeen: Int,
    replacement: TabledEntity
  )

  /**
   * @param result
   *   Its fields are sorted by column name, so the same superset always yields the identical
   *   `StructType`.
   * @param columnNames
   *   The columns the batch's own events use, which are the ones the window writes. It holds an
   *   entity's main column whenever the batch carries the entity, including when the batch carries
   *   only versions of it that the type does not cover.
   * @param notStored
   *   Newcomers that took the fallback because the superset was full.
   */
  final case class Resolved(
    result: NonAtomicFields.Result,
    columnNames: Set[String],
    notStored: Set[TabledEntity],
    displaced: List[Displacement],
    size: Int
  )

  /**
   * @param resolveTypes
   *   A parameter so the calls can be counted and failures tested without a broken registry.
   */
  def resolve[F[_]: Sync](
    resolveTypes: ResolveTypes[F],
    schemasToSkip: List[SchemaCriterion],
    superset: Ref[F, Superset],
    capacity: Int,
    window: Int,
    batch: Entities
  ): F[Either[ResolveTypesException, Resolved]] =
    superset.get.flatMap { known =>
      val wanted = withoutSkipped(schemasToSkip, batch)
      val toResolve = wanted.filter { case (te, versions) =>
        !known.get(te).exists(e => versions.subsetOf(e.versions))
      }

      resolveOrEmpty(resolveTypes, toResolve).flatMap {
        case Left(e) =>
          Sync[F].pure(Left(e))
        case Right(result) =>
          val fresh = result.fields.map { tte =>
            tte.tabledEntity -> Entry(covered(tte), tte, window)
          }.toMap
          val candidates = wanted.keySet.flatMap { te =>
            fresh.get(te).orElse(known.get(te)).map(te -> _)
          }.toMap
          val reported = result.igluFailures.map { f =>
            f.copy(versionsInBatch = f.versionsInBatch -- known.get(f.tabledEntity).fold(Set.empty[SchemaSubVersion])(_.versions))
          }
          superset
            .modify { current =>
              checkClashes((current ++ candidates).values) match {
                case Left(e) => (current, Left(e))
                case Right(_) =>
                  val admitted = admit(current, capacity, window, candidates)
                  (admitted.superset, Right(admitted))
              }
            }
            .map(_.map { admitted =>
              val batchFields = (admitted.superset ++ admitted.fallback).values.map(_.typed).toVector.sortBy(_.mergedField.name)
              Resolved(
                NonAtomicFields.Result(batchFields, reported),
                columnNamesForBatch(batchFields, wanted),
                admitted.notStored,
                admitted.displaced,
                admitted.superset.size
              )
            })
      }
    }

  private def resolveOrEmpty[F[_]: Sync](
    resolveTypes: ResolveTypes[F],
    entities: Entities
  ): F[Either[ResolveTypesException, NonAtomicFields.Result]] =
    if (entities.isEmpty) Sync[F].pure(NonAtomicFields.Result(Vector.empty, Nil).asRight)
    else resolveTypes(entities)

  /** The same filter `resolveTypes` applies before its lookups. */
  private def withoutSkipped(schemasToSkip: List[SchemaCriterion], batch: Entities): Entities =
    batch
      .map { case (te, versions) =>
        te -> versions.filterNot(v => schemasToSkip.exists(_.matches(TabledEntity.toSchemaKey(te, v))))
      }
      .filter(_._2.nonEmpty)

  private def covered(tte: TypedTabledEntity): Set[SchemaSubVersion] =
    tte.mergedVersions ++ tte.recoveries.map(_._1)

  private final case class Admitted(
    superset: Superset,
    fallback: Superset,
    notStored: Set[TabledEntity],
    displaced: List[Displacement]
  )

  private def admit(
    current: Superset,
    capacity: Int,
    window: Int,
    candidates: Superset
  ): Admitted = {
    val (stored, newcomers) = candidates.partition { case (te, _) => current.contains(te) }

    val afterStored = stored.foldLeft(Admitted(current, Map.empty, Set.empty, Nil)) { case (acc, (te, candidate)) =>
      val cur  = acc.superset(te)
      val seen = math.max(cur.lastSeen, window)
      if (candidate.versions.subsetOf(cur.versions))
        acc.copy(superset = acc.superset.updated(te, cur.copy(lastSeen = seen)))
      else if (cur.versions.subsetOf(candidate.versions))
        acc.copy(superset = acc.superset.updated(te, candidate.copy(lastSeen = seen)))
      else
        acc.copy(superset = acc.superset.updated(te, cur.copy(lastSeen = seen)), fallback = acc.fallback.updated(te, candidate))
    }

    val ordered = newcomers.toVector.sortBy(_._2.typed.mergedField.name)
    val admitted = ordered.foldLeft(afterStored) { case (acc, (te, candidate)) =>
      val entry = candidate.copy(lastSeen = window)
      if (acc.superset.size < capacity)
        acc.copy(superset = acc.superset.updated(te, entry))
      else
        coldest(acc.superset, window) match {
          case Some(evicted) =>
            val displacement = Displacement(evicted, acc.superset(evicted).lastSeen, te)
            acc.copy(superset = acc.superset.removed(evicted).updated(te, entry), displaced = displacement :: acc.displaced)
          case None =>
            acc.copy(fallback = acc.fallback.updated(te, candidate), notStored = acc.notStored + te)
        }
    }
    admitted.copy(displaced = admitted.displaced.reverse)
  }

  private def coldest(superset: Superset, window: Int): Option[TabledEntity] =
    superset.iterator
      .filter(_._2.lastSeen < window - 1)
      .minByOption { case (_, entry) => (entry.lastSeen, entry.typed.mergedField.name) }
      .map(_._1)

  private def checkClashes(entries: Iterable[Entry]): Either[ResolveTypesException, Unit] = {
    val clashes = entries
      .groupBy(_.typed.mergedField.name)
      .values
      .filter(_.size > 1)
      .map(_.map(_.typed.tabledEntity).toSet)
      .toSet
    if (clashes.isEmpty) Right(()) else Left(ResolveTypesException.ClashingFieldNames(clashes))
  }

  private def columnNamesForBatch(fields: Vector[TypedTabledEntity], batch: Entities): Set[String] =
    fields.flatMap { tte =>
      batch.get(tte.tabledEntity) match {
        case Some(versions) =>
          tte.mergedField.name :: tte.recoveries.collect { case (v, field) if versions.contains(v) => field.name }
        case None =>
          Nil
      }
    }.toSet
}
