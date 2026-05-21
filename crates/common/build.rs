use prost::Message;
use prost_types::{
    DescriptorProto, FieldDescriptorProto, FileDescriptorSet, MessageOptions,
    field_descriptor_proto::{Label, Type},
};
use std::{
    collections::{HashMap, HashSet},
    env,
    fs::File,
    io::{Read, Write},
    path::Path,
};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("cargo:rerun-if-env-changed=DEP_TEMPORALIO_PROTOS_DESCRIPTOR_PATH");
    let out = std::path::PathBuf::from(env::var("OUT_DIR").unwrap());
    let descriptor_file = std::path::PathBuf::from(
        env::var("DEP_TEMPORALIO_PROTOS_DESCRIPTOR_PATH")
            .map_err(|_| "temporalio-protos did not publish descriptor metadata")?,
    );

    generate_payload_visitor(&out, &descriptor_file)?;
    Ok(())
}

/// Generate PayloadVisitable implementations by parsing proto descriptors.
fn generate_payload_visitor(
    out_dir: &Path,
    descriptor_path: &Path,
) -> Result<(), Box<dyn std::error::Error>> {
    let mut descriptor_bytes = Vec::new();
    File::open(descriptor_path)?.read_to_end(&mut descriptor_bytes)?;
    let descriptor_set = FileDescriptorSet::decode(&descriptor_bytes[..])?;

    let mut generator = PayloadVisitorGenerator::new();
    generator.process_descriptors(&descriptor_set);

    let output_path = out_dir.join("payload_visitor_impl.rs");
    let mut file = File::create(&output_path)?;
    file.write_all(generator.generate().as_bytes())?;

    Ok(())
}

/// Stores information about a message field that contains payloads.
#[derive(Debug, Clone)]
struct PayloadFieldInfo {
    /// The proto field name
    name: String,
    /// The fully qualified proto path for the field
    proto_path: String,
    /// What kind of payload field this is
    kind: PayloadFieldKind,
}

#[derive(Debug, Clone)]
enum PayloadFieldKind {
    /// A singular Payload field
    SinglePayload,
    /// A repeated Payload field
    RepeatedPayload,
    /// A Payloads message field
    PayloadsMessage,
    /// A map with Payload values
    MapPayload,
    /// A map with nested message values that contain payloads
    MapNestedMessage,
    /// A nested message that contains payloads
    NestedMessage,
    /// A oneof that may contain payloads
    Oneof {
        /// The name of the oneof field
        oneof_name: String,
        /// Payload-containing variants
        variants: Vec<OneofVariant>,
        /// Total number of variants in the oneof (to know if we need a catch-all)
        total_variants: usize,
    },
}

#[derive(Debug, Clone)]
struct OneofVariant {
    name: String,
}

/// Generator for PayloadVisitable implementations.
struct PayloadVisitorGenerator {
    /// Maps fully qualified message names to their descriptors
    messages: HashMap<String, DescriptorProto>,
    /// Messages that contain Payloads (directly or transitively)
    payload_containing: HashSet<String>,
    /// Types currently being checked (for cycle detection)
    checking: HashSet<String>,
    /// Types that have been checked and don't contain payloads
    not_payload_containing: HashSet<String>,
    /// The payload fields for each message
    message_fields: HashMap<String, Vec<PayloadFieldInfo>>,
}

impl PayloadVisitorGenerator {
    fn new() -> Self {
        Self {
            messages: HashMap::new(),
            payload_containing: HashSet::new(),
            checking: HashSet::new(),
            not_payload_containing: HashSet::new(),
            message_fields: HashMap::new(),
        }
    }

    fn process_descriptors(&mut self, descriptor_set: &FileDescriptorSet) {
        // First pass: collect all message types
        for file in &descriptor_set.file {
            let package = file.package.as_deref().unwrap_or("");
            for msg in &file.message_type {
                self.collect_messages(package, msg);
            }
        }

        // Second pass: find payload-containing types
        let all_names: Vec<String> = self.messages.keys().cloned().collect();
        for name in &all_names {
            self.check_contains_payload(name);
        }

        // Third pass: build field info for payload-containing types
        for name in self.payload_containing.clone() {
            self.build_field_info(&name);
        }
    }

    fn collect_messages(&mut self, package: &str, msg: &DescriptorProto) {
        let name = msg.name.as_deref().unwrap_or("");
        let full_name = if package.is_empty() {
            name.to_string()
        } else {
            format!("{}.{}", package, name)
        };

        self.messages.insert(full_name.clone(), msg.clone());

        // Collect nested types
        for nested in &msg.nested_type {
            // Skip map entry types
            if is_map_entry(&nested.options) {
                continue;
            }
            self.collect_messages(&full_name, nested);
        }
    }

    fn check_contains_payload(&mut self, name: &str) -> bool {
        // Already determined to contain payloads
        if self.payload_containing.contains(name) {
            return true;
        }

        // Already determined to not contain payloads
        if self.not_payload_containing.contains(name) {
            return false;
        }

        // Currently checking this type - break the cycle
        if self.checking.contains(name) {
            return false;
        }

        // Base cases
        if name == "temporal.api.common.v1.Payload" {
            self.payload_containing.insert(name.to_string());
            return true;
        }
        if name == "temporal.api.common.v1.Payloads" {
            self.payload_containing.insert(name.to_string());
            return true;
        }

        let msg = match self.messages.get(name) {
            Some(m) => m.clone(),
            None => return false,
        };

        // Mark as currently checking
        self.checking.insert(name.to_string());

        // Check each field
        for field in &msg.field {
            if self.field_contains_payload(&msg, field) {
                self.checking.remove(name);
                self.payload_containing.insert(name.to_string());
                return true;
            }
        }

        // Done checking - doesn't contain payloads
        self.checking.remove(name);
        self.not_payload_containing.insert(name.to_string());
        false
    }

    fn field_contains_payload(
        &mut self,
        msg: &DescriptorProto,
        field: &FieldDescriptorProto,
    ) -> bool {
        if is_message_type(field) {
            let type_name = field.type_name.as_deref().unwrap_or("");
            let type_name = type_name.trim_start_matches('.');

            // Check if this is a map type
            if let Some(nested) = msg.nested_type.iter().find(|n| {
                is_map_entry(&n.options)
                    && n.name.as_deref()
                        == Some(&Self::to_map_entry_name(
                            field.name.as_deref().unwrap_or(""),
                        ))
            }) {
                // It's a map - check the value type
                if let Some(value_field) = nested
                    .field
                    .iter()
                    .find(|f| f.name.as_deref() == Some("value"))
                {
                    let value_type = value_field
                        .type_name
                        .as_deref()
                        .unwrap_or("")
                        .trim_start_matches('.');
                    return self.check_contains_payload(value_type);
                }
            }

            return self.check_contains_payload(type_name);
        }

        false
    }

    fn to_map_entry_name(field_name: &str) -> String {
        let mut result = String::new();
        let mut capitalize_next = true;
        for c in field_name.chars() {
            if c == '_' {
                capitalize_next = true;
            } else if capitalize_next {
                result.push(c.to_ascii_uppercase());
                capitalize_next = false;
            } else {
                result.push(c);
            }
        }
        result.push_str("Entry");
        result
    }

    fn build_field_info(&mut self, name: &str) {
        if self.message_fields.contains_key(name) {
            return;
        }

        // Skip Payload and Payloads - they are leaf types
        if name == "temporal.api.common.v1.Payload" || name == "temporal.api.common.v1.Payloads" {
            return;
        }

        let msg = match self.messages.get(name) {
            Some(m) => m.clone(),
            None => return,
        };

        let mut fields = Vec::new();

        // Group fields by oneof
        let mut oneof_fields: HashMap<i32, Vec<&FieldDescriptorProto>> = HashMap::new();
        let mut regular_fields: Vec<&FieldDescriptorProto> = Vec::new();

        for field in &msg.field {
            if let Some(oneof_index) = field.oneof_index {
                oneof_fields.entry(oneof_index).or_default().push(field);
            } else {
                regular_fields.push(field);
            }
        }

        // Process regular fields
        for field in regular_fields {
            if let Some(info) = self.build_single_field_info(name, &msg, field) {
                fields.push(info);
            }
        }

        // Process oneofs
        for (oneof_index, oneof_field_list) in oneof_fields {
            let oneof_desc = &msg.oneof_decl[oneof_index as usize];
            let oneof_name = oneof_desc.name.as_deref().unwrap_or("");

            let total_variants = oneof_field_list.len();
            let mut variants = Vec::new();
            for field in oneof_field_list {
                if is_message_type(field) {
                    let type_name = field
                        .type_name
                        .as_deref()
                        .unwrap_or("")
                        .trim_start_matches('.');
                    if self.payload_containing.contains(type_name) {
                        variants.push(OneofVariant {
                            name: field.name.clone().unwrap_or_default(),
                        });
                    }
                }
            }

            if !variants.is_empty() {
                fields.push(PayloadFieldInfo {
                    name: oneof_name.to_string(),
                    proto_path: format!("{}.{}", name, oneof_name),
                    kind: PayloadFieldKind::Oneof {
                        oneof_name: oneof_name.to_string(),
                        variants,
                        total_variants,
                    },
                });
            }
        }

        self.message_fields.insert(name.to_string(), fields);
    }

    fn build_single_field_info(
        &self,
        parent_name: &str,
        parent_msg: &DescriptorProto,
        field: &FieldDescriptorProto,
    ) -> Option<PayloadFieldInfo> {
        let field_name = field.name.as_deref().unwrap_or("");
        let proto_path = format!("{}.{}", parent_name, field_name);

        if !is_message_type(field) {
            return None;
        }

        let type_name = field
            .type_name
            .as_deref()
            .unwrap_or("")
            .trim_start_matches('.');

        // Check if it's a map
        if let Some(nested) = parent_msg.nested_type.iter().find(|n| {
            is_map_entry(&n.options)
                && n.name.as_deref() == Some(&Self::to_map_entry_name(field_name))
        }) {
            let value_field = nested
                .field
                .iter()
                .find(|f| f.name.as_deref() == Some("value"))?;
            let value_type = value_field
                .type_name
                .as_deref()
                .unwrap_or("")
                .trim_start_matches('.');

            if !self.payload_containing.contains(value_type) {
                return None;
            }

            if value_type == "temporal.api.common.v1.Payload" {
                return Some(PayloadFieldInfo {
                    name: field_name.to_string(),
                    proto_path,
                    kind: PayloadFieldKind::MapPayload,
                });
            } else {
                return Some(PayloadFieldInfo {
                    name: field_name.to_string(),
                    proto_path,
                    kind: PayloadFieldKind::MapNestedMessage,
                });
            }
        }

        if !self.payload_containing.contains(type_name) {
            return None;
        }

        let is_repeated = is_repeated(field);

        if type_name == "temporal.api.common.v1.Payload" {
            Some(PayloadFieldInfo {
                name: field_name.to_string(),
                proto_path,
                kind: if is_repeated {
                    PayloadFieldKind::RepeatedPayload
                } else {
                    PayloadFieldKind::SinglePayload
                },
            })
        } else if type_name == "temporal.api.common.v1.Payloads" {
            Some(PayloadFieldInfo {
                name: field_name.to_string(),
                proto_path,
                kind: PayloadFieldKind::PayloadsMessage,
            })
        } else {
            Some(PayloadFieldInfo {
                name: field_name.to_string(),
                proto_path,
                kind: PayloadFieldKind::NestedMessage,
            })
        }
    }

    fn generate(&self) -> String {
        let mut output = String::new();
        output.push_str("// Generated from descriptors.bin - DO NOT EDIT\n\n");

        // Generate impls for each payload-containing type
        for name in self.payload_containing.iter() {
            if name == "temporal.api.common.v1.Payload" || name == "temporal.api.common.v1.Payloads"
            {
                continue;
            }
            if let Some(fields) = self.message_fields.get(name) {
                output.push_str(&self.generate_impl(name, fields));
                output.push('\n');
            }
        }

        output
    }

    fn generate_impl(&self, proto_name: &str, fields: &[PayloadFieldInfo]) -> String {
        let rust_path = self.proto_to_rust_path(proto_name);

        let mut impl_body = String::new();

        for field in fields {
            impl_body.push_str(&self.generate_field_visit(
                &field.name,
                &field.proto_path,
                &field.kind,
            ));
        }

        format!(
            r#"#[allow(deprecated, clippy::single_match, clippy::collapsible_match)]
impl crate::payload_visitor::PayloadVisitable for {rust_path} {{
    fn visit_payloads_mut<'a>(
        &'a mut self,
        visitor: &'a mut (dyn crate::payload_visitor::AsyncPayloadVisitor + Send),
    ) -> futures::future::BoxFuture<'a, ()> {{
        Box::pin(async move {{
{impl_body}        }})
    }}
}}
"#,
            rust_path = rust_path,
            impl_body = impl_body
        )
    }

    fn generate_field_visit(
        &self,
        field_name: &str,
        proto_path: &str,
        kind: &PayloadFieldKind,
    ) -> String {
        let rust_field = Self::to_snake_case(field_name);

        match kind {
            PayloadFieldKind::SinglePayload => {
                format!(
                    r#"        if let Some(payload) = &mut self.{field} {{
            visitor.visit(crate::payload_visitor::PayloadField {{
                path: "{path}",
                data: crate::payload_visitor::PayloadFieldData::Single(payload),
            }}).await;
        }}
"#,
                    field = rust_field,
                    path = proto_path
                )
            }
            PayloadFieldKind::RepeatedPayload => {
                format!(
                    r#"        visitor.visit(crate::payload_visitor::PayloadField {{
            path: "{path}",
            data: crate::payload_visitor::PayloadFieldData::Repeated(&mut self.{field}),
        }}).await;
"#,
                    field = rust_field,
                    path = proto_path
                )
            }
            PayloadFieldKind::PayloadsMessage => {
                format!(
                    r#"        if let Some(payloads) = &mut self.{field} {{
            visitor.visit(crate::payload_visitor::PayloadField {{
                path: "{path}",
                data: crate::payload_visitor::PayloadFieldData::Payloads(payloads),
            }}).await;
        }}
"#,
                    field = rust_field,
                    path = proto_path
                )
            }
            PayloadFieldKind::MapPayload => {
                format!(
                    r#"        for payload in self.{field}.values_mut() {{
            visitor.visit(crate::payload_visitor::PayloadField {{
                path: "{path}",
                data: crate::payload_visitor::PayloadFieldData::Single(payload),
            }}).await;
        }}
"#,
                    field = rust_field,
                    path = proto_path
                )
            }
            PayloadFieldKind::MapNestedMessage => {
                format!(
                    r#"        for item in self.{field}.values_mut() {{
            item.visit_payloads_mut(visitor).await;
        }}
"#,
                    field = rust_field
                )
            }
            PayloadFieldKind::NestedMessage => {
                // Check if the field in the parent is repeated
                let parent_name = proto_path.rsplit_once('.').map(|(p, _)| p).unwrap_or("");
                let is_field_repeated = if let Some(msg) = self.messages.get(parent_name) {
                    msg.field
                        .iter()
                        .any(|f| f.name.as_deref() == Some(field_name) && is_repeated(f))
                } else {
                    false
                };

                if is_field_repeated {
                    format!(
                        r#"        for item in &mut self.{field} {{
            item.visit_payloads_mut(visitor).await;
        }}
"#,
                        field = rust_field
                    )
                } else {
                    format!(
                        r#"        if let Some(msg) = &mut self.{field} {{
            msg.visit_payloads_mut(visitor).await;
        }}
"#,
                        field = rust_field
                    )
                }
            }
            PayloadFieldKind::Oneof {
                oneof_name,
                variants,
                total_variants,
            } => {
                // Compute the parent proto name from the proto_path
                let parent_proto_name = proto_path.rsplit_once('.').map(|(p, _)| p).unwrap_or("");
                // Get the full rust path to the oneof enum
                let enum_path = self.proto_to_rust_oneof_enum_path(parent_proto_name, oneof_name);
                // The field in the struct is snake_case of the oneof field name
                let rust_field = Self::to_snake_case(oneof_name);

                let mut arms = String::new();

                for variant in variants {
                    let variant_name = Self::to_pascal_case(&variant.name);
                    arms.push_str(&format!(
                        "                {enum_path}::{variant}(msg) => msg.visit_payloads_mut(visitor).await,\n",
                        enum_path = enum_path,
                        variant = variant_name
                    ));
                }

                if arms.is_empty() {
                    return String::new();
                }

                // Only add catch-all if not all variants are payload-containing
                let catch_all = if variants.len() < *total_variants {
                    "                _ => {}\n"
                } else {
                    ""
                };

                format!(
                    r#"        if let Some({field}) = &mut self.{field} {{
            match {field} {{
{arms}{catch_all}            }}
        }}
"#,
                    field = rust_field,
                    arms = arms,
                    catch_all = catch_all
                )
            }
        }
    }

    fn proto_to_rust_path(&self, proto_name: &str) -> String {
        let parts: Vec<&str> = proto_name.split('.').collect();
        let mut rust_parts = Vec::new();

        // Handle the package -> module mapping
        for (i, part) in parts.iter().enumerate() {
            if i == parts.len() - 1 {
                // Last part is the type name - keep PascalCase
                rust_parts.push((*part).to_string());
            } else {
                // Package parts become snake_case modules
                rust_parts.push(Self::to_snake_case(part));
            }
        }

        // The protos module structure
        let path = rust_parts.join("::");

        // Map to the actual crate paths
        format!("crate::protos::{}", path)
    }

    fn proto_to_rust_oneof_enum_path(&self, parent_proto_name: &str, oneof_name: &str) -> String {
        let parts: Vec<&str> = parent_proto_name.split('.').collect();
        let mut rust_parts = Vec::new();

        // All parts become snake_case modules (struct name becomes a module containing the enum)
        for part in parts.iter() {
            rust_parts.push(Self::to_snake_case(part));
        }

        let module_path = rust_parts.join("::");
        // The enum name is PascalCase of the oneof field name
        let enum_name = Self::to_pascal_case(oneof_name);

        format!("crate::protos::{}::{}", module_path, enum_name)
    }

    fn to_snake_case(s: &str) -> String {
        let mut result = String::new();
        for (i, c) in s.chars().enumerate() {
            if c.is_uppercase() {
                if i > 0 {
                    result.push('_');
                }
                result.push(c.to_ascii_lowercase());
            } else {
                result.push(c);
            }
        }
        result
    }

    fn to_pascal_case(s: &str) -> String {
        let mut result = String::new();
        let mut capitalize_next = true;
        for c in s.chars() {
            if c == '_' {
                capitalize_next = true;
            } else if capitalize_next {
                result.push(c.to_ascii_uppercase());
                capitalize_next = false;
            } else {
                result.push(c);
            }
        }
        result
    }
}

fn is_message_type(field: &FieldDescriptorProto) -> bool {
    field.r#type == Some(Type::Message as i32)
}

fn is_repeated(field: &FieldDescriptorProto) -> bool {
    field.label == Some(Label::Repeated as i32)
}

fn is_map_entry(options: &Option<MessageOptions>) -> bool {
    options
        .as_ref()
        .is_some_and(|o| o.map_entry.unwrap_or(false))
}
