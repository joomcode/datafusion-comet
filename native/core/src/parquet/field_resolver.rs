// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

//! Parquet field resolution for one group: the root schema or one struct.
//!
//! Spark's `ParquetReadSupport.clipParquetGroupFields` resolves every requested field of a group
//! on its own against the file fields of that group. A requested field that carries a Parquet
//! field id, when id matching is on, resolves by id only (`matchIdField`): a missing id is a
//! missing column, never a name match. Every other requested field resolves by name, folded with
//! `toLowerCase(Locale.ROOT)` when the read is case-insensitive. Comet resolves fields in three
//! places: the root [`schema_adapter`](super::schema_adapter), which renames file columns so
//! DataFusion's exact-name lookup finds them; the nested `Struct -> Struct` convert in
//! [`parquet_support`](super::parquet_support); and the plan-time conversion check, which
//! resolves nested fields the way that convert does. All of them select file fields through
//! [`FieldResolver`], so a change to the precedence rules reaches the root and nested fields
//! together.

use crate::parquet::name_fold::fold_names;
use crate::parquet::parquet_support::{duplicate_parquet_field_error, field_id};
use arrow::datatypes::{DataType, FieldRef};
use datafusion::common::{DataFusionError, Result as DataFusionResult};
use datafusion_comet_common::SparkError;
use std::collections::HashMap;

/// The file fields that one requested field selects, as indices in file order.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Selection<'a> {
    /// Spark's `matchIdField`: id matching is on and the requested field carries an id, so it
    /// selects the file field that carries the same id and never compares names. `None` means
    /// the column is missing.
    ById(Option<usize>),
    /// Spark's `matchCaseSensitiveField` / `matchCaseInsensitiveField`: the file fields whose
    /// folded name equals the requested field's folded name.
    ByName(&'a [usize]),
}

/// Resolves the requested fields of one group against the file fields of the same group, as
/// Spark's `clipParquetGroupFields` does. The caller folds both sides' names once (see
/// `name_fold`) and passes them in, parallel to the fields.
///
/// [`FieldResolver::select`] raises a requested id that more than one file field carries, which
/// both the root and nested fields report at once, as Spark does. It does not raise an
/// ambiguous name: the root reports one only for a column the read references, and
/// [`FieldResolver::resolve`] raises it for a nested field.
pub(crate) struct FieldResolver<'a> {
    requested: &'a [FieldRef],
    requested_folded: &'a [String],
    file: &'a [FieldRef],
    file_folded: &'a [String],
    case_sensitive: bool,
    /// File field indices by Parquet field id (Spark's `idToParquetFieldMap`), present only when
    /// id matching is on and some requested field carries an id (Spark's `shouldMatchById`).
    /// Spark's check also counts ids on nested fields, but only a field that carries an id itself
    /// resolves by id, so this group's own fields decide.
    file_by_id: Option<HashMap<i32, Indices>>,
    /// File field indices by folded name, grouped so a collision is seen rather than overwritten.
    /// Spark's `caseInsensitiveParquetFieldMap`, and its case-sensitive map when the fold is the
    /// identity.
    file_by_name: HashMap<&'a str, Indices>,
}

impl<'a> FieldResolver<'a> {
    pub(crate) fn new(
        requested: &'a [FieldRef],
        requested_folded: &'a [String],
        file: &'a [FieldRef],
        file_folded: &'a [String],
        case_sensitive: bool,
        use_field_id: bool,
    ) -> Self {
        debug_assert_eq!(requested.len(), requested_folded.len());
        debug_assert_eq!(file.len(), file_folded.len());
        let file_by_id =
            (use_field_id && requested.iter().any(|f| field_id(f).is_some())).then(|| {
                let mut file_by_id = HashMap::with_capacity(file.len());
                for (index, field) in file.iter().enumerate() {
                    if let Some(id) = field_id(field) {
                        Indices::insert(&mut file_by_id, id, index);
                    }
                }
                file_by_id
            });
        let mut file_by_name = HashMap::with_capacity(file_folded.len());
        for (index, folded) in file_folded.iter().enumerate() {
            Indices::insert(&mut file_by_name, folded.as_str(), index);
        }
        Self {
            requested,
            requested_folded,
            file,
            file_folded,
            case_sensitive,
            file_by_id,
            file_by_name,
        }
    }

    pub(crate) fn requested_fields(&self) -> &'a [FieldRef] {
        self.requested
    }

    pub(crate) fn requested_folded(&self) -> &'a [String] {
        self.requested_folded
    }

    pub(crate) fn file_fields(&self) -> &'a [FieldRef] {
        self.file
    }

    pub(crate) fn file_folded(&self) -> &'a [String] {
        self.file_folded
    }

    /// Whether some requested field resolves by Parquet field id (Spark's `shouldMatchById`).
    pub(crate) fn matches_by_id(&self) -> bool {
        self.file_by_id.is_some()
    }

    /// The Parquet field id that requested field `requested` resolves by, or `None` when it
    /// resolves by name.
    pub(crate) fn requested_id(&self, requested: usize) -> Option<i32> {
        if self.matches_by_id() {
            field_id(&self.requested[requested])
        } else {
            None
        }
    }

    /// The file fields requested field `requested` selects, by id when it carries one and id
    /// matching is on, otherwise by folded name. A requested id that more than one file field
    /// carries raises Spark's `_LEGACY_ERROR_TEMP_2094`.
    pub(crate) fn select(&self, requested: usize) -> DataFusionResult<Selection<'_>> {
        let Some(id) = self.requested_id(requested) else {
            return Ok(Selection::ByName(self.name_matches(requested)));
        };
        let matches = self
            .file_by_id
            .as_ref()
            .and_then(|file_by_id| file_by_id.get(&id))
            .map_or(&[][..], Indices::as_slice);
        match matches {
            [] => Ok(Selection::ById(None)),
            [index] => Ok(Selection::ById(Some(*index))),
            _ => Err(SparkError::DuplicateFieldByFieldId {
                required_id: id,
                // Bracketed and comma-joined the way Spark's `matchIdField` renders the list, so
                // the message reads `id=1 matches [x, y]` on both sides.
                matched_fields: format!("[{}]", field_names(self.file, matches).join(", ")),
            }
            .into()),
        }
    }

    /// The file fields whose folded name equals requested field `requested`'s, even when it
    /// selects by id. The root hides these behind placeholder names so that a field `matchIdField`
    /// resolves cannot reach them by name.
    pub(crate) fn name_matches(&self, requested: usize) -> &[usize] {
        self.file_by_name
            .get(self.requested_folded[requested].as_str())
            .map_or(&[], Indices::as_slice)
    }

    /// `folded name -> file field indices` for every folded name more than one file field shares,
    /// or `None` when no names collide.
    pub(crate) fn name_collisions(&self) -> Option<HashMap<String, Vec<usize>>> {
        let collisions: HashMap<String, Vec<usize>> = self
            .file_by_name
            .iter()
            .filter_map(|(name, indices)| match indices {
                Indices::Many(all) => Some((name.to_string(), all.clone())),
                Indices::One(_) => None,
            })
            .collect();
        (!collisions.is_empty()).then_some(collisions)
    }

    /// The file field that requested field `requested` reads from, or `None` when the file holds
    /// no such field. An ambiguous selection is an error, raised before a decoder can multiply
    /// rows: see [`FieldResolver::select`] for a repeated id and [`duplicate_name_error`] for a
    /// name.
    pub(crate) fn resolve(&self, requested: usize) -> DataFusionResult<Option<usize>> {
        match self.select(requested)? {
            Selection::ById(index) => Ok(index),
            Selection::ByName([]) => Ok(None),
            Selection::ByName([index]) => Ok(Some(*index)),
            Selection::ByName(matches) => Err(duplicate_name_error(
                self.requested[requested].name(),
                self.file,
                matches,
                self.case_sensitive,
            )),
        }
    }
}

/// Validate the fields a scan actually requests, before any pages are decoded.
/// Spark's SPARK-60262 clipping guard rejects one physical field selected twice,
/// including an id match beside a name match. Validate the projected schema,
/// not the table's full schema: an unreferenced alias does not read the column.
pub(crate) fn validate_field_selection(
    file: &[FieldRef],
    requested: &[FieldRef],
    case_sensitive: bool,
    use_field_id: bool,
) -> DataFusionResult<()> {
    let names: Vec<&str> = file
        .iter()
        .chain(requested)
        .map(|field| field.name().as_str())
        .collect();
    let folded = fold_names(&names, case_sensitive)?;
    let (file_folded, requested_folded) = folded.split_at(file.len());
    let resolver = FieldResolver::new(
        requested,
        requested_folded,
        file,
        file_folded,
        case_sensitive,
        use_field_id,
    );
    // Spark clips each field recursively in request order, then checks this
    // group's repetitions. Preserve that order: a nested failure must not be
    // hidden by a root repetition or a later sibling's failed lookup.
    let mut selected = Vec::with_capacity(requested.len());
    for (index, field) in requested.iter().enumerate() {
        let physical = resolver.resolve(index)?;
        if let Some(physical) = physical {
            validate_nested_selection(
                file[physical].data_type(),
                field.data_type(),
                case_sensitive,
                use_field_id,
            )?;
        }
        selected.push(physical);
    }
    let mut seen = std::collections::HashSet::new();
    for &physical in selected.iter().flatten() {
        // Spark checks the exact clipped Parquet name, not the ordinal or
        // folded name. Distinct-id fields named A/A repeat, while A/a do not.
        let physical_name = file[physical].name();
        if !seen.insert(physical_name.as_str()) {
            return Err(SparkError::ParquetFieldRequestedMoreThanOnce {
                requested_fields: requested
                    .iter()
                    .zip(&selected)
                    .filter(|(_, selection)| {
                        selection.is_some_and(|index| file[index].name() == physical_name)
                    })
                    .map(|(field, _)| field.name().clone())
                    .collect(),
                parquet_field: file[physical].name().clone(),
            }
            .into());
        }
    }
    Ok(())
}

fn validate_nested_selection(
    file: &DataType,
    requested: &DataType,
    case_sensitive: bool,
    use_field_id: bool,
) -> DataFusionResult<()> {
    match (file, requested) {
        (DataType::Dictionary(_, value), _) => {
            validate_nested_selection(value, requested, case_sensitive, use_field_id)
        }
        (DataType::Struct(file), DataType::Struct(requested)) => {
            validate_field_selection(file, requested, case_sensitive, use_field_id)
        }
        (
            DataType::List(file)
            | DataType::LargeList(file)
            | DataType::FixedSizeList(file, _)
            | DataType::ListView(file)
            | DataType::LargeListView(file),
            DataType::List(requested)
            | DataType::LargeList(requested)
            | DataType::FixedSizeList(requested, _)
            | DataType::ListView(requested)
            | DataType::LargeListView(requested),
        ) => validate_nested_selection(
            file.data_type(),
            requested.data_type(),
            case_sensitive,
            use_field_id,
        ),
        (DataType::Map(file, _), DataType::Map(requested, _)) => {
            // Map key/value wrappers pair positionally, as Spark clips them;
            // only structs inside a key or value resolve fields by name/id.
            if let (DataType::Struct(file), DataType::Struct(requested)) =
                (file.data_type(), requested.data_type())
            {
                for (file, requested) in file.iter().zip(requested) {
                    validate_nested_selection(
                        file.data_type(),
                        requested.data_type(),
                        case_sensitive,
                        use_field_id,
                    )?;
                }
            }
            Ok(())
        }
        _ => Ok(()), // Existing conversion validation handles differing shapes.
    }
}

/// The error for requested field `name` when the file fields at `matches` all share its folded
/// name: Spark's `foundDuplicateFieldInCaseInsensitiveModeError` when the read is
/// case-insensitive, and Comet's duplicate field name error for a repeated exact name, where Spark
/// would read the last field of that name.
pub(crate) fn duplicate_name_error(
    name: &str,
    file: &[FieldRef],
    matches: &[usize],
    case_sensitive: bool,
) -> DataFusionError {
    if case_sensitive {
        return duplicate_parquet_field_error(name);
    }
    DataFusionError::External(Box::new(SparkError::duplicate_field_case_insensitive(
        name,
        &field_names(file, matches),
    )))
}

fn field_names<'f>(fields: &'f [FieldRef], indices: &[usize]) -> Vec<&'f str> {
    indices
        .iter()
        .map(|&index| fields[index].name().as_str())
        .collect()
}

/// The file field indices that share one key, in file order. Almost every key is unique, so a
/// single index is kept inline, and a group allocates only when two fields collide.
#[derive(Debug)]
enum Indices {
    One(usize),
    Many(Vec<usize>),
}

impl Indices {
    fn insert<K: std::hash::Hash + Eq>(map: &mut HashMap<K, Indices>, key: K, index: usize) {
        map.entry(key)
            .and_modify(|indices| match indices {
                Indices::One(first) => *indices = Indices::Many(vec![*first, index]),
                Indices::Many(all) => all.push(index),
            })
            .or_insert(Indices::One(index));
    }

    fn as_slice(&self) -> &[usize] {
        match self {
            Indices::One(index) => std::slice::from_ref(index),
            Indices::Many(all) => all,
        }
    }
}

#[cfg(test)]
mod test {
    use super::{validate_field_selection, FieldResolver, Selection};
    use crate::parquet::name_fold::fold_names;
    use arrow::datatypes::{DataType, Field, FieldRef};
    use datafusion::common::DataFusionError;
    use datafusion_comet_common::SparkError;
    use parquet::arrow::PARQUET_FIELD_ID_META_KEY;
    use std::collections::HashMap;
    use std::sync::Arc;

    fn field(name: &str, id: Option<i32>) -> FieldRef {
        let field = Field::new(name, DataType::Int32, true);
        Arc::new(match id {
            Some(id) => field.with_metadata(HashMap::from([(
                PARQUET_FIELD_ID_META_KEY.to_string(),
                id.to_string(),
            )])),
            None => field,
        })
    }

    fn folded(fields: &[FieldRef], case_sensitive: bool) -> Vec<String> {
        let names: Vec<&str> = fields.iter().map(|f| f.name().as_str()).collect();
        fold_names(&names, case_sensitive).unwrap()
    }

    /// Run `check` against a resolver over `requested` and `file`.
    fn with_resolver<T>(
        requested: &[FieldRef],
        file: &[FieldRef],
        case_sensitive: bool,
        use_field_id: bool,
        check: impl FnOnce(&FieldResolver) -> T,
    ) -> T {
        let requested_folded = folded(requested, case_sensitive);
        let file_folded = folded(file, case_sensitive);
        check(&FieldResolver::new(
            requested,
            &requested_folded,
            file,
            &file_folded,
            case_sensitive,
            use_field_id,
        ))
    }

    /// `matchIdField` selects by id even when another file field has the requested name.
    #[test]
    fn id_bearing_field_selects_by_id_over_name() {
        let requested = [field("a", Some(1))];
        let file = [field("a", Some(2)), field("b", Some(1))];
        with_resolver(&requested, &file, false, true, |resolver| {
            assert_eq!(resolver.select(0).unwrap(), Selection::ById(Some(1)));
            assert_eq!(resolver.resolve(0).unwrap(), Some(1));
            assert_eq!(resolver.name_matches(0), &[0]);
        });
    }

    /// `matchIdField` treats an id the file lacks as a missing column (Spark gives it a fake
    /// name) instead of falling back to the file field with the requested name.
    #[test]
    fn missing_id_is_a_missing_column() {
        let requested = [field("a", Some(1))];
        let file = [field("A", Some(2))];
        with_resolver(&requested, &file, false, true, |resolver| {
            assert_eq!(resolver.select(0).unwrap(), Selection::ById(None));
            assert_eq!(resolver.resolve(0).unwrap(), None);
        });
    }

    /// `shouldMatchById` only routes fields that carry an id to `matchIdField`. A field without one
    /// resolves by name even when its siblings resolve by id, and may select a file field that also
    /// carries an id.
    #[test]
    fn field_without_id_resolves_by_name_beside_id_bearing_siblings() {
        let requested = [field("a", Some(1)), field("b", None)];
        let file = [field("B", Some(5))];
        with_resolver(&requested, &file, false, true, |resolver| {
            assert_eq!(resolver.select(1).unwrap(), Selection::ByName(&[0]));
            assert_eq!(resolver.resolve(1).unwrap(), Some(0));
        });
    }

    /// With id matching off, ids on either side are ignored.
    #[test]
    fn ids_are_ignored_when_id_matching_is_off() {
        let requested = [field("a", Some(1))];
        let file = [field("x", Some(1)), field("a", Some(2))];
        with_resolver(&requested, &file, false, false, |resolver| {
            assert_eq!(resolver.select(0).unwrap(), Selection::ByName(&[1]));
        });
    }

    fn repeated_selection(error: DataFusionError) -> (Vec<String>, String) {
        let DataFusionError::External(source) = error else {
            panic!("expected structured repeated-field error, got {error}");
        };
        let Some(SparkError::ParquetFieldRequestedMoreThanOnce {
            requested_fields,
            parquet_field,
        }) = source.downcast_ref::<SparkError>()
        else {
            panic!("unexpected error: {source}");
        };
        (requested_fields.clone(), parquet_field.clone())
    }

    #[test]
    fn repeated_field_id_and_name_selection_is_rejected_in_both_orders_and_case_modes() {
        let file = [field("A", Some(1))];
        for (case_sensitive, by_name) in [(true, "A"), (false, "a")] {
            for reverse in [false, true] {
                let mut requested = vec![field("x", Some(1)), field(by_name, None)];
                if reverse {
                    requested.reverse();
                }
                let error =
                    validate_field_selection(&file, &requested, case_sensitive, true).unwrap_err();
                assert_eq!(
                    repeated_selection(error),
                    (
                        requested.iter().map(|field| field.name().clone()).collect(),
                        "A".into()
                    )
                );
                // An unreferenced alias must not reject a projected read.
                for projected in requested.chunks(1) {
                    validate_field_selection(&file, projected, case_sensitive, true).unwrap();
                }
            }
        }
    }

    #[test]
    fn repeated_field_id_selection_is_rejected_but_missing_and_distinct_fields_are_valid() {
        let file = [field("A", Some(1)), field("B", Some(2))];
        let repeated = [field("x", Some(1)), field("y", Some(1))];
        assert_eq!(
            repeated_selection(
                validate_field_selection(&file, &repeated, false, true).unwrap_err()
            ),
            (vec!["x".into(), "y".into()], "A".into())
        );
        let distinct = [
            field("x", Some(1)),
            field("b", None),
            field("missing", Some(9)),
        ];
        validate_field_selection(&file, &distinct, false, true).unwrap();
        // With id reads off, x is missing and a reads the file by name once.
        let name_only = [field("x", Some(1)), field("a", None)];
        validate_field_selection(&file, &name_only, false, false).unwrap();
    }

    #[test]
    fn repeated_case_folded_name_selection_matches_the_running_spark_contract() {
        let file = [field("A", None)];
        let requested = [field("a", None), field("A", None)];
        assert_eq!(
            repeated_selection(
                validate_field_selection(&file, &requested, false, false).unwrap_err()
            ),
            (vec!["a".into(), "A".into()], "A".into())
        );
        validate_field_selection(&file, &requested, true, false).unwrap();
    }

    #[test]
    fn repeated_selection_is_checked_below_struct_list_and_map_without_rejecting_pruning() {
        let physical = DataType::Struct(vec![field("A", Some(1))].into());
        let repeated = DataType::Struct(vec![field("x", Some(1)), field("a", None)].into());
        let pruned = DataType::Struct(vec![field("x", Some(1))].into());
        let wrappers: Vec<Box<dyn Fn(DataType) -> DataType>> = vec![
            Box::new(|value| value),
            Box::new(|value| DataType::List(Arc::new(Field::new("element", value, true)))),
            Box::new(|value| DataType::LargeList(Arc::new(Field::new("element", value, true)))),
            Box::new(|value| {
                DataType::Map(
                    Arc::new(Field::new(
                        "entries",
                        DataType::Struct(
                            vec![
                                Arc::new(Field::new("key", DataType::Utf8, false)),
                                Arc::new(Field::new("value", value, true)),
                            ]
                            .into(),
                        ),
                        false,
                    )),
                    false,
                )
            }),
        ];
        for wrap in wrappers {
            let file = [Arc::new(Field::new("s", wrap(physical.clone()), true))];
            let requested = [Arc::new(Field::new("s", wrap(repeated.clone()), true))];
            assert_eq!(
                repeated_selection(
                    validate_field_selection(&file, &requested, false, true).unwrap_err()
                ),
                (vec!["x".into(), "a".into()], "A".into())
            );
            let requested = [Arc::new(Field::new("s", wrap(pruned.clone()), true))];
            validate_field_selection(&file, &requested, false, true).unwrap();
        }
    }

    #[test]
    fn nested_selection_errors_precede_group_repetitions_and_later_lookup_errors() {
        let physical_struct = Arc::new(
            Field::new(
                "s",
                DataType::Struct(vec![field("A", Some(1))].into()),
                true,
            )
            .with_metadata(field("s", Some(10)).metadata().clone()),
        );
        let requested_struct = Arc::new(
            Field::new(
                "s",
                DataType::Struct(vec![field("nested_x", Some(1)), field("a", None)].into()),
                true,
            )
            .with_metadata(field("s", Some(10)).metadata().clone()),
        );
        // The current group repeats B before visiting the struct. Spark still
        // reports the struct's A collision, since clipping recurses before the
        // current group's repeated-column check.
        let file = [field("B", Some(20)), Arc::clone(&physical_struct)];
        let requested = [
            field("root_x", Some(20)),
            field("b", None),
            Arc::clone(&requested_struct),
        ];
        assert_eq!(
            repeated_selection(
                validate_field_selection(&file, &requested, false, true).unwrap_err()
            ),
            (vec!["nested_x".into(), "a".into()], "A".into())
        );
        // A later sibling has an ambiguous field-id lookup, which must not
        // hide the earlier struct's collision either.
        let file = [physical_struct, field("B", Some(20)), field("C", Some(20))];
        let requested = [requested_struct, field("root_x", Some(20))];
        assert_eq!(
            repeated_selection(
                validate_field_selection(&file, &requested, false, true).unwrap_err()
            ),
            (vec!["nested_x".into(), "a".into()], "A".into())
        );
    }

    #[test]
    fn repeated_selection_uses_exact_physical_names_not_ordinals_or_folded_names() {
        let requested = [field("x", Some(1)), field("y", Some(2))];
        let repeated_names = [field("A", Some(1)), field("A", Some(2))];
        assert_eq!(
            repeated_selection(
                validate_field_selection(&repeated_names, &requested, false, true).unwrap_err()
            ),
            (vec!["x".into(), "y".into()], "A".into())
        );
        // ID matches are unambiguous despite the case-insensitive name index.
        // Spark's clipped-name repetition check is case-sensitive.
        let distinct_names = [field("A", Some(1)), field("a", Some(2))];
        validate_field_selection(&distinct_names, &requested, false, true).unwrap();
        validate_field_selection(&distinct_names, &requested, true, true).unwrap();
    }
}
