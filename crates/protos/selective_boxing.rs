//! Research heuristic for default Prost storage on a 64-bit target, not a Rust ABI model.
use prost_descriptor::{
    DescriptorProto, FieldDescriptorProto, FileDescriptorSet,
    field_descriptor_proto::{Label, Type},
};
use std::collections::{BTreeMap, BTreeSet};

#[derive(Debug)]
pub(crate) struct Selection {
    pub(crate) path: String,
    pub(crate) reason: String,
}

#[derive(Clone, Copy, Debug)]
pub(crate) struct Options {
    pub(crate) field_threshold: usize,
    pub(crate) minimum_saving: usize,
    pub(crate) message_budget: usize,
}
impl Default for Options {
    fn default() -> Self {
        Self {
            field_threshold: 128,
            minimum_saving: 64,
            message_budget: 512,
        }
    }
}
/// Approximate default Prost storage on a 64-bit target. This assumes owned
/// String/Vec, HashMap, no extern paths or custom field attributes, and eight-byte
/// scalar rounding; it deliberately makes no Rust ABI or speed guarantee.
/// Select children before parents so an already-small child is not boxed again.
/// Apply paths with Config::boxed before compile_fds.
pub(crate) fn select_with_options(fds: &FileDescriptorSet, options: Options) -> Vec<Selection> {
    let arm_threshold = options.field_threshold;
    let minimum_saving = options.minimum_saving;
    let mut messages = BTreeMap::new();
    for file in &fds.file {
        let prefix = if file.package().is_empty() {
            String::new()
        } else {
            format!(".{}", file.package())
        };
        for m in &file.message_type {
            collect(m, &prefix, &mut messages);
        }
    }
    let mut model = Model {
        messages,
        boxed: BTreeSet::new(),
    };
    let mut order = Vec::new();
    let mut seen = BTreeSet::new();
    for name in model.messages.keys() {
        model.order(name, &mut seen, &mut order);
    }
    let mut result = Vec::new();
    for name in order {
        let m = model.messages[&name];
        if m.options.as_ref().is_some_and(|o| o.map_entry()) {
            continue;
        }
        for (index, oneof) in m.oneof_decl.iter().enumerate() {
            let arms: Vec<_> = m
                .field
                .iter()
                .filter(|f| f.oneof_index == Some(index as i32) && !f.proto3_optional())
                .collect();
            let sizes: Option<Vec<_>> = arms
                .iter()
                .map(|f| model.field(&name, f, true, &mut BTreeSet::new()))
                .collect();
            let Some(sizes) = sizes else {
                continue;
            };
            let before = sizes.iter().copied().max().unwrap_or(0);
            let candidates: Vec<_> = arms
                .iter()
                .zip(&sizes)
                .filter(|(f, s)| {
                    f.r#type() == Type::Message
                        && f.label() != Label::Repeated
                        && **s > arm_threshold
                        && !model.recursive(&name, f.type_name())
                })
                .collect();
            let after = arms
                .iter()
                .zip(&sizes)
                .map(|(f, s)| {
                    if candidates.iter().any(|(c, _)| c.name() == f.name()) {
                        8
                    } else {
                        *s
                    }
                })
                .max()
                .unwrap_or(0);
            if candidates.is_empty() || before.saturating_sub(after) < minimum_saving {
                continue;
            }
            for (f, size) in candidates {
                let path = format!("{name}.{}.{}", oneof.name(), f.name());
                model.boxed.insert(path.clone());
                result.push(Selection { path, reason: format!("estimated arm {size} B > {arm_threshold} B; group maximum {before} -> {after} B (saving {} B >= {minimum_saving} B)", before-after) });
            }
        }
        while let Some(before) = model.message(&name, &mut BTreeSet::new()) {
            if before <= options.message_budget {
                break;
            }
            let mut candidates: Vec<_> = m
                .field
                .iter()
                .filter(|f| {
                    (f.oneof_index.is_none() || f.proto3_optional())
                        && f.r#type() == Type::Message
                        && f.label() != Label::Repeated
                        && !model.recursive(&name, f.type_name())
                })
                .filter_map(|f| {
                    model
                        .field(&name, f, false, &mut BTreeSet::new())
                        .map(|size| (f, size))
                })
                .filter(|(_, size)| {
                    *size > arm_threshold && size.saturating_sub(8) >= minimum_saving
                })
                .collect();
            candidates.sort_by(|(a, sa), (b, sb)| sb.cmp(sa).then_with(|| a.name().cmp(b.name())));
            let Some((f, size)) = candidates.first() else {
                break;
            };
            let path = format!("{name}.{}", f.name());
            model.boxed.insert(path.clone());
            result.push(Selection { path, reason: format!("estimated owner {before} B exceeds {} B budget; largest eligible singular field {size} -> 8 B", options.message_budget) });
        }
    }
    result.sort_by(|a, b| a.path.cmp(&b.path));
    result
}

fn collect<'a>(
    m: &'a DescriptorProto,
    prefix: &str,
    result: &mut BTreeMap<String, &'a DescriptorProto>,
) {
    let name = format!("{prefix}.{}", m.name());
    result.insert(name.clone(), m);
    for nested in &m.nested_type {
        collect(nested, &name, result);
    }
}
struct Model<'a> {
    messages: BTreeMap<String, &'a DescriptorProto>,
    boxed: BTreeSet<String>,
}
impl Model<'_> {
    fn order(&self, name: &str, seen: &mut BTreeSet<String>, order: &mut Vec<String>) {
        if !seen.insert(name.to_owned()) {
            return;
        }
        if let Some(m) = self.messages.get(name) {
            let targets: BTreeSet<_> = m
                .field
                .iter()
                .filter(|f| {
                    f.r#type() == Type::Message
                        && f.label() != Label::Repeated
                        && !self.recursive(name, f.type_name())
                })
                .map(|f| f.type_name())
                .collect();
            for target in targets {
                self.order(target, seen, order);
            }
        }
        order.push(name.to_owned());
    }

    fn recursive(&self, owner: &str, target: &str) -> bool {
        let mut pending = vec![target];
        let mut seen = BTreeSet::new();
        while let Some(name) = pending.pop() {
            if name == owner {
                return true;
            }
            if !seen.insert(name) {
                continue;
            }
            if let Some(m) = self.messages.get(name) {
                pending.extend(
                    m.field
                        .iter()
                        .filter(|f| f.r#type() == Type::Message && f.label() != Label::Repeated)
                        .map(|f| f.type_name()),
                );
            }
        }
        false
    }
    fn message(&self, name: &str, visiting: &mut BTreeSet<String>) -> Option<usize> {
        let m = self.messages.get(name)?;
        if !visiting.insert(name.to_owned()) {
            return None;
        }
        let mut sum = 0;
        for f in &m.field {
            if f.oneof_index.is_none() || f.proto3_optional() {
                sum += self.field(name, f, false, visiting)?;
            }
        }
        for i in 0..m.oneof_decl.len() {
            let mut largest = None;
            for f in m
                .field
                .iter()
                .filter(|f| f.oneof_index == Some(i as i32) && !f.proto3_optional())
            {
                largest = Some(
                    largest
                        .unwrap_or(0)
                        .max(self.field(name, f, true, visiting)?),
                );
            }
            if let Some(size) = largest {
                sum += size + 8;
            }
        }
        visiting.remove(name);
        Some(sum)
    }
    fn field(
        &self,
        owner: &str,
        f: &FieldDescriptorProto,
        oneof: bool,
        visiting: &mut BTreeSet<String>,
    ) -> Option<usize> {
        let path = if oneof {
            let m = self.messages.get(owner)?;
            format!(
                "{owner}.{}.{}",
                m.oneof_decl[f.oneof_index? as usize].name(),
                f.name()
            )
        } else {
            format!("{owner}.{}", f.name())
        };
        if self.boxed.contains(&path) {
            return Some(8);
        }
        if f.label() == Label::Repeated {
            let map = self
                .messages
                .get(f.type_name())
                .is_some_and(|m| m.options.as_ref().is_some_and(|o| o.map_entry()));
            return Some(if map { 48 } else { 24 });
        }
        let size = match f.r#type() {
            Type::Message => {
                if self.recursive(owner, f.type_name()) {
                    8
                } else {
                    self.message(f.type_name(), visiting)?
                        + if oneof || f.label() == Label::Required {
                            0
                        } else {
                            8
                        }
                }
            }
            Type::String | Type::Bytes => 24,
            Type::Group => return None,
            // Rounding scalars to one word intentionally trades precision for a simple model.
            _ => 8,
        };
        Some(
            size + if f.proto3_optional() && f.r#type() != Type::Message {
                8
            } else {
                0
            },
        )
    }
}
