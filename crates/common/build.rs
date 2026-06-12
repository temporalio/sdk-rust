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
    generate_payload_limits_validator(&out, &descriptor_file)?;
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

// ===========================================================================
// Payload-limits validator generator
//
// Emits `PayloadLimitsValidatable` impls for the closure of outbound gRPC request
// messages, dispatching each payload-bearing *leaf* field through a hand-authored
// decision tables (`*_FIELDS`, grouped by class) mapping `(message.field)` -> limit class.
// The *measurement strategy* is derived mechanically from each field's proto shape;
// the table only records the specified class decision (blob / memo / not).
//
// Any payload-bearing leaf field that is missing from the table -- or any stale table
// entry no longer produced by the descriptor walk -- fails the build. This is the
// compile-time forcing function: a proto change cannot land until classified here.
// ===========================================================================

/// Fully-qualified proto names whose value is a *measurable unit* the server checks as a whole.
/// The generator stops recursion at these and emits a table-driven leaf check at the parent field.
const TERMINAL_LEAVES: &[&str] = &[
    "temporal.api.common.v1.Payload",
    "temporal.api.common.v1.Payloads",
    "temporal.api.common.v1.Memo",
    "temporal.api.common.v1.Header",
    "temporal.api.common.v1.SearchAttributes",
    // The server size-checks a Failure as a whole proto (e.g. FailWorkflowExecution), so we measure
    // it as one unit rather than decomposing it into its inner payload fields.
    "temporal.api.failure.v1.Failure",
];

/// Field paths the server size-checks as a whole serialized sub-message even though the field is
/// not itself payload-bearing — so payload-reachability never reaches them. Measured via
/// `message_size` (the server's `proto.Size`), classified via the table like any other leaf. The
/// owning message is forced into the validated closure so parents recurse into it.
const EXTRA_WHOLE_MESSAGE_LEAVES: &[&str] = &[
    // protocol Message body (google.protobuf.Any): the server blob-checks `proto.Size(message.Body)`
    // when processing update messages and fails the WFT on exceed (handleMessage).
    "temporal.api.protocol.v1.Message.body",
];

// Payload-limits decision tables — THE source of truth for how the SDK mirrors the server's
// payload/memo size checks.
//   - Blob          blob (payload) size limit, warn + error
//   - Memo          memo size limit, warn + error
//   - BlobWarn      blob limit, warning only
//   - NotValidated  the server does not enforce a *replicable* limit on this field
//
// Roots are derived automatically from the proto service definitions (every `temporal.api.*` RPC
// *input* message; see `service_request_roots`), so a new RPC/request can't be silently missed — its
// payload fields become unclassified and fail the build until added below. The measurement strategy
// is derived mechanically from the field's proto shape.
const BLOB_FIELDS: &[&str] = &[
    "temporal.api.command.v1.CompleteWorkflowExecutionCommandAttributes.result",
    "temporal.api.command.v1.ContinueAsNewWorkflowExecutionCommandAttributes.input",
    "temporal.api.command.v1.FailWorkflowExecutionCommandAttributes.failure", // whole Failure proto
    "temporal.api.command.v1.ModifyWorkflowPropertiesCommandAttributes.upserted_memo", // memo fields data-sum
    "temporal.api.command.v1.RecordMarkerCommandAttributes.details", // map<string,Payloads> sum
    "temporal.api.command.v1.ScheduleActivityTaskCommandAttributes.input",
    "temporal.api.command.v1.ScheduleNexusOperationCommandAttributes.input",
    "temporal.api.command.v1.SignalExternalWorkflowExecutionCommandAttributes.input",
    "temporal.api.command.v1.StartChildWorkflowExecutionCommandAttributes.input",
    "temporal.api.command.v1.UpsertWorkflowSearchAttributesCommandAttributes.search_attributes", // indexed_fields data-sum
    "temporal.api.protocol.v1.Message.body", // whole Any body; see EXTRA_WHOLE_MESSAGE_LEAVES
    "temporal.api.query.v1.WorkflowQuery.query_args",
    "temporal.api.workflow.v1.NewWorkflowExecutionInfo.input",
    "temporal.api.workflowservice.v1.RecordActivityTaskHeartbeatByIdRequest.details",
    "temporal.api.workflowservice.v1.RecordActivityTaskHeartbeatRequest.details",
    "temporal.api.workflowservice.v1.RespondActivityTaskCanceledByIdRequest.details",
    "temporal.api.workflowservice.v1.RespondActivityTaskCanceledRequest.details",
    "temporal.api.workflowservice.v1.RespondActivityTaskCompletedByIdRequest.result",
    "temporal.api.workflowservice.v1.RespondActivityTaskCompletedRequest.result",
    "temporal.api.workflowservice.v1.SignalWithStartWorkflowExecutionRequest.input",
    "temporal.api.workflowservice.v1.SignalWithStartWorkflowExecutionRequest.signal_input",
    "temporal.api.workflowservice.v1.SignalWorkflowExecutionRequest.input",
    "temporal.api.workflowservice.v1.StartActivityExecutionRequest.input",
    "temporal.api.workflowservice.v1.StartNexusOperationExecutionRequest.input",
    "temporal.api.workflowservice.v1.StartWorkflowExecutionRequest.input",
];

const MEMO_FIELDS: &[&str] = &[
    "temporal.api.command.v1.ContinueAsNewWorkflowExecutionCommandAttributes.memo",
    "temporal.api.command.v1.StartChildWorkflowExecutionCommandAttributes.memo",
    "temporal.api.workflow.v1.NewWorkflowExecutionInfo.memo",
    "temporal.api.workflowservice.v1.SignalWithStartWorkflowExecutionRequest.memo",
    "temporal.api.workflowservice.v1.StartWorkflowExecutionRequest.memo",
];

// Warn-only: the SDK warns but never proactively fails the task (failure responses; query results).
const BLOB_WARN_FIELDS: &[&str] = &[
    "temporal.api.workflowservice.v1.RespondActivityTaskFailedByIdRequest.failure",
    "temporal.api.workflowservice.v1.RespondActivityTaskFailedByIdRequest.last_heartbeat_details",
    "temporal.api.workflowservice.v1.RespondActivityTaskFailedRequest.failure",
    "temporal.api.workflowservice.v1.RespondActivityTaskFailedRequest.last_heartbeat_details",
    "temporal.api.workflowservice.v1.RespondNexusTaskFailedRequest.failure",
    "temporal.api.workflowservice.v1.RespondWorkflowTaskFailedRequest.failure",
    "temporal.api.query.v1.WorkflowQueryResult.answer",
    "temporal.api.workflowservice.v1.RespondQueryTaskCompletedRequest.query_result",
];

const NOT_VALIDATED_FIELDS: &[&str] = &[
    // Headers: server records a HeaderSize metric only.
    "temporal.api.batch.v1.BatchOperationSignal.header",
    "temporal.api.command.v1.ContinueAsNewWorkflowExecutionCommandAttributes.header",
    "temporal.api.command.v1.RecordMarkerCommandAttributes.header",
    "temporal.api.command.v1.ScheduleActivityTaskCommandAttributes.header",
    "temporal.api.command.v1.SignalExternalWorkflowExecutionCommandAttributes.header",
    "temporal.api.command.v1.StartChildWorkflowExecutionCommandAttributes.header",
    "temporal.api.query.v1.WorkflowQuery.header",
    "temporal.api.update.v1.Input.header",
    "temporal.api.workflow.v1.NewWorkflowExecutionInfo.header",
    "temporal.api.workflow.v1.PostResetOperation.SignalWorkflow.header",
    "temporal.api.workflowservice.v1.SignalWithStartWorkflowExecutionRequest.header",
    "temporal.api.workflowservice.v1.SignalWorkflowExecutionRequest.header",
    "temporal.api.workflowservice.v1.StartActivityExecutionRequest.header",
    "temporal.api.workflowservice.v1.StartWorkflowExecutionRequest.header",
    // Search attributes: separate non-replicable SA limit (server merges with the workflow's existing SAs).
    "temporal.api.command.v1.ContinueAsNewWorkflowExecutionCommandAttributes.search_attributes",
    "temporal.api.command.v1.StartChildWorkflowExecutionCommandAttributes.search_attributes",
    "temporal.api.workflow.v1.NewWorkflowExecutionInfo.search_attributes",
    "temporal.api.workflowservice.v1.CreateScheduleRequest.search_attributes",
    "temporal.api.workflowservice.v1.SignalWithStartWorkflowExecutionRequest.search_attributes",
    "temporal.api.workflowservice.v1.StartActivityExecutionRequest.search_attributes",
    "temporal.api.workflowservice.v1.StartNexusOperationExecutionRequest.search_attributes",
    "temporal.api.workflowservice.v1.StartWorkflowExecutionRequest.search_attributes",
    "temporal.api.workflowservice.v1.UpdateScheduleRequest.search_attributes",
    // Internal carry-over fields the SDK doesn't author / the server doesn't size-check here.
    "temporal.api.command.v1.CancelWorkflowExecutionCommandAttributes.details",
    "temporal.api.command.v1.ContinueAsNewWorkflowExecutionCommandAttributes.failure",
    "temporal.api.command.v1.ContinueAsNewWorkflowExecutionCommandAttributes.last_completion_result",
    "temporal.api.command.v1.RecordMarkerCommandAttributes.failure",
    "temporal.api.workflowservice.v1.StartWorkflowExecutionRequest.continued_failure",
    "temporal.api.workflowservice.v1.StartWorkflowExecutionRequest.last_completion_result",
    "temporal.api.workflowservice.v1.TerminateWorkflowExecutionRequest.details",
    // Dedicated, non-fetchable limits (not blob/memo, not in DescribeNamespace): UserMetadata
    // (nexus-start only); Nexus EndpointSpec.description (maxDescriptionSize; cloud variant cloud-only).
    "temporal.api.sdk.v1.UserMetadata.details",
    "temporal.api.sdk.v1.UserMetadata.summary",
    "temporal.api.nexus.v1.EndpointSpec.description",
    "temporal.api.cloud.nexus.v1.EndpointSpec.description",
    // Update input args: frontend records a metric only — enforced on delivery via Message.body.
    "temporal.api.update.v1.Input.args",
    // Query/nexus failures and the nexus sync response payload: not size-checked on these paths.
    "temporal.api.nexus.v1.StartOperationResponse.Sync.payload",
    "temporal.api.nexus.v1.StartOperationResponse.failure",
    "temporal.api.query.v1.WorkflowQueryResult.failure",
    "temporal.api.workflowservice.v1.RespondQueryTaskCompletedRequest.failure",
    // Schedules: server sums memo + action.input vs blob — cross-field aggregate (Custom, deferred).
    "temporal.api.workflowservice.v1.CreateScheduleRequest.memo",
    "temporal.api.workflowservice.v1.UpdateScheduleRequest.memo",
    // Enforced downstream: signal input is blob-checked per target on batch/reset fan-out.
    "temporal.api.batch.v1.BatchOperationSignal.input",
    "temporal.api.workflow.v1.PostResetOperation.SignalWorkflow.input",
    // Not size-checked by the server.
    "temporal.api.batch.v1.BatchOperationTermination.details",
    "temporal.api.deployment.v1.UpdateDeploymentMetadata.upsert_entries",
    "temporal.api.workflowservice.v1.UpdateWorkerDeploymentVersionMetadataRequest.upsert_entries",
    // Cloud-only API; not handled by the OSS server.
    "temporal.api.compute.v1.ComputeProvider.details",
    "temporal.api.compute.v1.ComputeScaler.details",
];

/// The roots of the validated closure are derived automatically from the proto service definitions:
/// every RPC *input* (request) message of every `temporal.api.*` service. This guarantees a new RPC
/// or request message cannot be silently missed — its payload fields become unclassified and fail
/// the build until the table is updated. We use RPC inputs (not all messages) because the SDK only
/// ever *sends* requests; responses/history are received, never transmitted, so they need no limits.
fn service_request_roots(descriptor_set: &FileDescriptorSet) -> Vec<String> {
    let mut roots = Vec::new();
    for file in &descriptor_set.file {
        // Only Temporal API services (WorkflowService, OperatorService); excludes internal coresdk.
        if !file
            .package
            .as_deref()
            .unwrap_or("")
            .starts_with("temporal.api.")
        {
            continue;
        }
        for service in &file.service {
            for method in &service.method {
                if let Some(input) = method.input_type.as_deref() {
                    roots.push(input.trim_start_matches('.').to_string());
                }
            }
        }
    }
    roots.sort();
    roots.dedup();
    roots
}

/// The limit policy a payload field is subject to, per the decision table.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum LimitClass {
    /// Blob limit, warn + error.
    Blob,
    /// Memo limit, warn + error.
    Memo,
    /// Blob limit, warning only.
    BlobWarn,
    /// The server does not enforce a (replicable) limit on this field; recorded explicitly.
    NotValidated,
}

impl LimitClass {
    /// The generated `(LimitClass token, enforce_error)` pair, or `None` for `NotValidated`.
    fn token(self) -> Option<(&'static str, bool)> {
        match self {
            Self::Blob => Some(("crate::payload_limits::LimitClass::Blob", true)),
            Self::Memo => Some(("crate::payload_limits::LimitClass::Memo", true)),
            Self::BlobWarn => Some(("crate::payload_limits::LimitClass::Blob", false)),
            Self::NotValidated => None,
        }
    }
}

/// What a (non-oneof) field resolves to for limit purposes.
enum Target {
    /// A measurable leaf requiring a table entry at the field's proto path.
    Leaf(LeafKind),
    /// A payload-containing nested message to recurse into.
    Struct(StructShape, String),
    /// Not payload-bearing; ignored.
    Skip,
}

#[derive(Clone, Copy)]
enum LeafKind {
    SinglePayloads,
    SinglePayload,
    RepeatedPayload,
    SingleMemo,
    /// A `Memo` validated against the *blob* limit: measured as the data-sum of its `fields`, the
    /// way the server checks e.g. `ModifyWorkflowProperties.upserted_memo` (whereas a memo-classed
    /// `Memo` keeps whole-proto `memo_size`).
    MemoFieldsDataSum,
    SingleHeader,
    SingleSearchAttributes,
    MapPayload,
    MapPayloads,
    /// A whole message measured by its serialized proto size (e.g. Failure).
    WholeMessage,
    /// A repeated whole message, summed (e.g. repeated Failure).
    RepeatedWholeMessage,
}

#[derive(Clone, Copy)]
enum StructShape {
    Single,
    Repeated,
    Map,
}

fn generate_payload_limits_validator(
    out_dir: &Path,
    descriptor_path: &Path,
) -> Result<(), Box<dyn std::error::Error>> {
    let mut descriptor_bytes = Vec::new();
    File::open(descriptor_path)?.read_to_end(&mut descriptor_bytes)?;
    let descriptor_set = FileDescriptorSet::decode(&descriptor_bytes[..])?;

    // Reuse the visitor generator's first two passes for message collection + payload reachability.
    let mut base = PayloadVisitorGenerator::new();
    for file in &descriptor_set.file {
        let package = file.package.as_deref().unwrap_or("");
        for msg in &file.message_type {
            base.collect_messages(package, msg);
        }
    }
    let all_names: Vec<String> = base.messages.keys().cloned().collect();
    for name in &all_names {
        base.check_contains_payload(name);
    }

    // Force the owners of extra whole-message leaves to be treated as validatable, so the closure
    // walk recurses into them (e.g. RespondWorkflowTaskCompletedRequest.messages -> Message.body).
    for path in EXTRA_WHOLE_MESSAGE_LEAVES {
        if let Some((owner, _)) = path.rsplit_once('.') {
            base.payload_containing.insert(owner.to_string());
        }
    }

    let table = load_payload_limits_table()?;
    let mut used_keys: HashSet<String> = HashSet::new();
    let mut unclassified: Vec<String> = Vec::new();

    // Compute the closure of structural messages reachable from the service request roots.
    let roots = service_request_roots(&descriptor_set);
    let to_generate = limits_closure(&base, &roots);

    let mut output = String::new();
    output.push_str("// Generated from descriptors.bin - DO NOT EDIT\n");
    output.push_str("// Payload-limits validators. Edit the *_FIELDS tables in build.rs to classify fields.\n\n");

    let mut generate_names: Vec<String> = to_generate.into_iter().collect();
    generate_names.sort();
    for name in &generate_names {
        output.push_str(&generate_limits_impl(
            &base,
            name,
            &table,
            &mut used_keys,
            &mut unclassified,
        ));
    }

    // Fail the build on any unclassified leaf field (forces an explicit decision on proto changes).
    if !unclassified.is_empty() {
        unclassified.sort();
        unclassified.dedup();
        let list = unclassified
            .iter()
            .map(|p| format!("    \"{p}\","))
            .collect::<Vec<_>>()
            .join("\n");
        return Err(format!(
            "payload-limits: {} payload-bearing field(s) are not classified. Add each to the right \
             *_FIELDS list (BLOB_FIELDS / MEMO_FIELDS / BLOB_WARN_FIELDS / NOT_VALIDATED_FIELDS) in \
             crates/common/build.rs:\n{}\n",
            unclassified.len(),
            list
        )
        .into());
    }

    // Fail the build on stale table entries (a field that no longer exists in the proto closure).
    let stale: Vec<&String> = table.keys().filter(|k| !used_keys.contains(*k)).collect();
    if !stale.is_empty() {
        let mut stale: Vec<String> = stale.into_iter().cloned().collect();
        stale.sort();
        return Err(format!(
            "payload-limits: {} stale entr(y/ies) in the *_FIELDS tables (crates/common/build.rs) \
             no longer correspond to a payload-bearing field; remove them:\n    {}\n",
            stale.len(),
            stale.join("\n    ")
        )
        .into());
    }

    let output_path = out_dir.join("payload_limits_impl.rs");
    File::create(&output_path)?.write_all(output.as_bytes())?;
    Ok(())
}

/// Load the decision table: `field proto path -> limit class`.
fn load_payload_limits_table() -> Result<HashMap<String, LimitClass>, Box<dyn std::error::Error>> {
    use std::iter::repeat;
    let entries = BLOB_FIELDS
        .iter()
        .zip(repeat(LimitClass::Blob))
        .chain(MEMO_FIELDS.iter().zip(repeat(LimitClass::Memo)))
        .chain(BLOB_WARN_FIELDS.iter().zip(repeat(LimitClass::BlobWarn)))
        .chain(
            NOT_VALIDATED_FIELDS
                .iter()
                .zip(repeat(LimitClass::NotValidated)),
        );
    let mut map = HashMap::new();
    for (path, class) in entries {
        if map.insert((*path).to_string(), class).is_some() {
            return Err(format!("payload-limits: duplicate table entry for `{path}`").into());
        }
    }
    Ok(map)
}

/// BFS from the roots, following payload-containing structural fields, collecting the message types
/// that need a generated `PayloadLimitsValidatable` impl (excludes terminal leaf types).
fn limits_closure(base: &PayloadVisitorGenerator, roots: &[String]) -> HashSet<String> {
    let mut result = HashSet::new();
    let mut queue: Vec<String> = roots.to_vec();
    while let Some(name) = queue.pop() {
        if TERMINAL_LEAVES.contains(&name.as_str()) || result.contains(&name) {
            continue;
        }
        let Some(msg) = base.messages.get(&name) else {
            continue;
        };
        if !base.payload_containing.contains(&name) {
            continue;
        }
        result.insert(name.clone());
        for field in &msg.field {
            if let Target::Struct(_, ty) = classify_field(base, msg, field) {
                queue.push(ty);
            } else if is_message_type(field) {
                // Oneof variants are message-typed; enqueue payload-containing structural variants.
                let ty = field
                    .type_name
                    .as_deref()
                    .unwrap_or("")
                    .trim_start_matches('.')
                    .to_string();
                if base.payload_containing.contains(&ty) && !TERMINAL_LEAVES.contains(&ty.as_str())
                {
                    queue.push(ty);
                }
            }
        }
    }
    result
}

/// Classify a (non-map-handling-aside) field into a leaf check or a structural recursion.
fn classify_field(
    base: &PayloadVisitorGenerator,
    parent_msg: &DescriptorProto,
    field: &FieldDescriptorProto,
) -> Target {
    if !is_message_type(field) {
        return Target::Skip;
    }
    let field_name = field.name.as_deref().unwrap_or("");
    let type_name = field
        .type_name
        .as_deref()
        .unwrap_or("")
        .trim_start_matches('.');

    // Map fields: classified by their value type.
    if let Some(nested) = parent_msg.nested_type.iter().find(|n| {
        is_map_entry(&n.options)
            && n.name.as_deref() == Some(&PayloadVisitorGenerator::to_map_entry_name(field_name))
    }) {
        let value_field = nested
            .field
            .iter()
            .find(|f| f.name.as_deref() == Some("value"));
        let value_type = value_field
            .and_then(|f| f.type_name.as_deref())
            .unwrap_or("")
            .trim_start_matches('.');
        return match value_type {
            "temporal.api.common.v1.Payload" => Target::Leaf(LeafKind::MapPayload),
            "temporal.api.common.v1.Payloads" => Target::Leaf(LeafKind::MapPayloads),
            other if base.payload_containing.contains(other) => {
                Target::Struct(StructShape::Map, other.to_string())
            }
            _ => Target::Skip,
        };
    }

    if !base.payload_containing.contains(type_name) {
        return Target::Skip;
    }

    let repeated = is_repeated(field);
    match type_name {
        "temporal.api.common.v1.Payload" => Target::Leaf(if repeated {
            LeafKind::RepeatedPayload
        } else {
            LeafKind::SinglePayload
        }),
        "temporal.api.common.v1.Payloads" => Target::Leaf(LeafKind::SinglePayloads),
        "temporal.api.common.v1.Memo" => Target::Leaf(LeafKind::SingleMemo),
        "temporal.api.common.v1.Header" => Target::Leaf(LeafKind::SingleHeader),
        "temporal.api.common.v1.SearchAttributes" => Target::Leaf(LeafKind::SingleSearchAttributes),
        "temporal.api.failure.v1.Failure" => Target::Leaf(if repeated {
            LeafKind::RepeatedWholeMessage
        } else {
            LeafKind::WholeMessage
        }),
        other => Target::Struct(
            if repeated {
                StructShape::Repeated
            } else {
                StructShape::Single
            },
            other.to_string(),
        ),
    }
}

fn generate_limits_impl(
    base: &PayloadVisitorGenerator,
    proto_name: &str,
    table: &HashMap<String, LimitClass>,
    used_keys: &mut HashSet<String>,
    unclassified: &mut Vec<String>,
) -> String {
    let conv = PayloadVisitorGenerator::new();
    let rust_path = conv.proto_to_rust_path(proto_name);
    let msg = &base.messages[proto_name];

    let mut body = String::new();

    // Group oneof fields; handle regular fields directly.
    let mut oneof_fields: HashMap<i32, Vec<&FieldDescriptorProto>> = HashMap::new();
    for field in &msg.field {
        if let Some(oneof_index) = field.oneof_index
            && !is_map_field(msg, field)
        {
            oneof_fields.entry(oneof_index).or_default().push(field);
            continue;
        }
        let field_name = field.name.as_deref().unwrap_or("");
        let proto_path = format!("{proto_name}.{field_name}");
        let rust_field = PayloadVisitorGenerator::to_snake_case(field_name);
        match classify_field(base, msg, field) {
            Target::Leaf(kind) => {
                body.push_str(&emit_leaf(
                    &proto_path,
                    field_name,
                    &rust_field,
                    kind,
                    table,
                    used_keys,
                    unclassified,
                ));
            }
            Target::Struct(shape, _) => {
                body.push_str(&emit_struct(&rust_field, field_name, shape));
            }
            Target::Skip => {}
        }
    }

    // Oneofs: recurse into payload-containing structural variants; leaf variants get a table check.
    let mut oneof_indices: Vec<i32> = oneof_fields.keys().copied().collect();
    oneof_indices.sort();
    for idx in oneof_indices {
        let variants = &oneof_fields[&idx];
        let oneof_name = msg.oneof_decl[idx as usize].name.as_deref().unwrap_or("");
        let mut arms = String::new();
        let mut payload_variants = 0usize;
        for field in variants {
            let var_field = field.name.as_deref().unwrap_or("");
            let type_name = field
                .type_name
                .as_deref()
                .unwrap_or("")
                .trim_start_matches('.');
            if !is_message_type(field) || !base.payload_containing.contains(type_name) {
                continue;
            }
            payload_variants += 1;
            let variant = PayloadVisitorGenerator::to_pascal_case(var_field);
            let enum_path = conv.proto_to_rust_oneof_enum_path(proto_name, oneof_name);
            if TERMINAL_LEAVES.contains(&type_name) {
                let proto_path = format!("{proto_name}.{var_field}");
                let kind = match type_name {
                    "temporal.api.common.v1.Payloads" => LeafKind::SinglePayloads,
                    "temporal.api.common.v1.Payload" => LeafKind::SinglePayload,
                    "temporal.api.common.v1.Memo" => LeafKind::SingleMemo,
                    "temporal.api.common.v1.Header" => LeafKind::SingleHeader,
                    "temporal.api.failure.v1.Failure" => LeafKind::WholeMessage,
                    _ => LeafKind::SingleSearchAttributes,
                };
                let check =
                    oneof_leaf_check(&proto_path, var_field, kind, table, used_keys, unclassified);
                arms.push_str(&format!(
                    "                {enum_path}::{variant}(inner) => {{ {check} }}\n"
                ));
            } else {
                arms.push_str(&format!(
                    "                {enum_path}::{variant}(inner) => {{\n                    sink.enter(\"{var_field}\", crate::payload_limits::FieldIndexer::None);\n                    crate::payload_limits::PayloadLimitsValidatable::validate_payload_limits(inner, sink);\n                    sink.exit();\n                }}\n"
                ));
            }
        }
        if payload_variants == 0 {
            continue;
        }
        let rust_field = PayloadVisitorGenerator::to_snake_case(oneof_name);
        // A catch-all is needed unless every variant is payload-bearing.
        let catch_all = if payload_variants < variants.len() {
            "                _ => {}\n"
        } else {
            ""
        };
        body.push_str(&format!(
            "        if let Some(oneof) = &self.{rust_field} {{\n            match oneof {{\n{arms}{catch_all}            }}\n        }}\n"
        ));
    }

    // Extra whole-message leaves: non-payload fields the server size-checks as a sub-message.
    for path in EXTRA_WHOLE_MESSAGE_LEAVES {
        let Some((owner, field_name)) = path.rsplit_once('.') else {
            continue;
        };
        if owner != proto_name {
            continue;
        }
        let rust_field = PayloadVisitorGenerator::to_snake_case(field_name);
        body.push_str(&emit_leaf(
            path,
            field_name,
            &rust_field,
            LeafKind::WholeMessage,
            table,
            used_keys,
            unclassified,
        ));
    }

    format!(
        r#"#[allow(deprecated, unused_variables, clippy::collapsible_if, clippy::collapsible_match, clippy::single_match)]
impl crate::payload_limits::PayloadLimitsValidatable for {rust_path} {{
    fn validate_payload_limits(&self, sink: &mut dyn crate::payload_limits::PayloadLimitSink) {{
{body}    }}
}}
"#
    )
}

/// Whether this field is the synthetic representation of a proto map (and thus not a real oneof).
fn is_map_field(parent_msg: &DescriptorProto, field: &FieldDescriptorProto) -> bool {
    let field_name = field.name.as_deref().unwrap_or("");
    parent_msg.nested_type.iter().any(|n| {
        is_map_entry(&n.options)
            && n.name.as_deref() == Some(&PayloadVisitorGenerator::to_map_entry_name(field_name))
    })
}

/// Resolve the table class for a leaf path, recording usage / lack of classification.
fn leaf_class(
    proto_path: &str,
    table: &HashMap<String, LimitClass>,
    used_keys: &mut HashSet<String>,
    unclassified: &mut Vec<String>,
) -> Option<LimitClass> {
    match table.get(proto_path) {
        Some(class) => {
            used_keys.insert(proto_path.to_string());
            Some(*class)
        }
        None => {
            unclassified.push(proto_path.to_string());
            None
        }
    }
}

/// The size expression for a leaf, given the name of the accessor binding already in scope.
fn leaf_size_expr(kind: LeafKind, accessor: &str) -> String {
    match kind {
        LeafKind::SinglePayloads => format!("crate::payload_limits::payloads_size({accessor})"),
        LeafKind::SinglePayload => format!("crate::payload_limits::payload_size({accessor})"),
        LeafKind::SingleMemo => format!("crate::payload_limits::memo_size({accessor})"),
        LeafKind::MemoFieldsDataSum => {
            format!("crate::payload_limits::map_payload_data_sum({accessor}.fields.iter())")
        }
        LeafKind::SingleHeader => {
            format!("crate::payload_limits::map_payload_data_sum({accessor}.fields.iter())")
        }
        LeafKind::SingleSearchAttributes => {
            format!("crate::payload_limits::map_payload_data_sum({accessor}.indexed_fields.iter())")
        }
        LeafKind::RepeatedPayload => {
            format!("{accessor}.iter().map(crate::payload_limits::payload_size).sum::<usize>()")
        }
        LeafKind::MapPayload => {
            format!("crate::payload_limits::map_payload_data_sum({accessor}.iter())")
        }
        LeafKind::MapPayloads => {
            format!("crate::payload_limits::map_payloads_sum({accessor}.iter())")
        }
        LeafKind::WholeMessage => format!("crate::payload_limits::message_size({accessor})"),
        LeafKind::RepeatedWholeMessage => {
            format!("{accessor}.iter().map(crate::payload_limits::message_size).sum::<usize>()")
        }
    }
}

/// Emit a leaf check for a regular (non-oneof) field.
fn emit_leaf(
    proto_path: &str,
    proto_field: &str,
    rust_field: &str,
    kind: LeafKind,
    table: &HashMap<String, LimitClass>,
    used_keys: &mut HashSet<String>,
    unclassified: &mut Vec<String>,
) -> String {
    let Some(class) = leaf_class(proto_path, table, used_keys, unclassified) else {
        return String::new();
    };
    let kind = effective_kind(kind, class);
    let Some((class_token, enforce)) = class.token() else {
        return String::new(); // NotValidated
    };
    match kind {
        // Optional message singulars: guard on Some.
        LeafKind::SinglePayloads
        | LeafKind::SinglePayload
        | LeafKind::SingleMemo
        | LeafKind::MemoFieldsDataSum
        | LeafKind::SingleHeader
        | LeafKind::SingleSearchAttributes
        | LeafKind::WholeMessage => {
            let size = leaf_size_expr(kind, "inner");
            format!(
                "        if let Some(inner) = &self.{rust_field} {{\n            sink.check(\"{proto_field}\", {class_token}, {size}, {enforce});\n        }}\n"
            )
        }
        // Repeated / map: always present (empty -> 0, harmless).
        LeafKind::RepeatedPayload
        | LeafKind::RepeatedWholeMessage
        | LeafKind::MapPayload
        | LeafKind::MapPayloads => {
            let accessor = format!("self.{rust_field}");
            let size = leaf_size_expr(kind, &accessor);
            format!("        sink.check(\"{proto_field}\", {class_token}, {size}, {enforce});\n")
        }
    }
}

/// Emit a leaf check inside a oneof match arm, where the variant payload is bound to `inner`.
fn oneof_leaf_check(
    proto_path: &str,
    proto_field: &str,
    kind: LeafKind,
    table: &HashMap<String, LimitClass>,
    used_keys: &mut HashSet<String>,
    unclassified: &mut Vec<String>,
) -> String {
    let Some(class) = leaf_class(proto_path, table, used_keys, unclassified) else {
        return String::new();
    };
    let kind = effective_kind(kind, class);
    let Some((class_token, enforce)) = class.token() else {
        return String::new();
    };
    let size = leaf_size_expr(kind, "inner");
    format!("sink.check(\"{proto_field}\", {class_token}, {size}, {enforce});")
}

/// Adjust a leaf's measurement for its classified limit. A `Memo` validated against the *blob*
/// limit is measured as the data-sum of its `fields` (the way the server checks
/// `ModifyWorkflowProperties.upserted_memo`); a memo-classed `Memo` keeps whole-proto `memo_size`.
fn effective_kind(kind: LeafKind, class: LimitClass) -> LeafKind {
    match (kind, class) {
        (LeafKind::SingleMemo, LimitClass::Blob | LimitClass::BlobWarn) => {
            LeafKind::MemoFieldsDataSum
        }
        _ => kind,
    }
}

/// Emit a structural recursion for a regular (non-oneof) field, pushing a path segment (proto field
/// name, plus index/key for repeated/map fields) around the descent.
fn emit_struct(rust_field: &str, proto_field: &str, shape: StructShape) -> String {
    match shape {
        StructShape::Single => format!(
            "        if let Some(inner) = &self.{rust_field} {{\n            sink.enter(\"{proto_field}\", crate::payload_limits::FieldIndexer::None);\n            crate::payload_limits::PayloadLimitsValidatable::validate_payload_limits(inner, sink);\n            sink.exit();\n        }}\n"
        ),
        StructShape::Repeated => format!(
            "        for (idx, inner) in self.{rust_field}.iter().enumerate() {{\n            sink.enter(\"{proto_field}\", crate::payload_limits::FieldIndexer::Index(idx));\n            crate::payload_limits::PayloadLimitsValidatable::validate_payload_limits(inner, sink);\n            sink.exit();\n        }}\n"
        ),
        StructShape::Map => format!(
            "        for (key, inner) in self.{rust_field}.iter() {{\n            sink.enter(\"{proto_field}\", crate::payload_limits::FieldIndexer::Key(key));\n            crate::payload_limits::PayloadLimitsValidatable::validate_payload_limits(inner, sink);\n            sink.exit();\n        }}\n"
        ),
    }
}
