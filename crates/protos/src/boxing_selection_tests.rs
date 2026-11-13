use crate::boxing_selection::{Options, select_with_options};
use prost_descriptor::{
    DescriptorProto, FieldDescriptorProto, FileDescriptorProto, FileDescriptorSet, MessageOptions,
    OneofDescriptorProto,
    field_descriptor_proto::{Label, Type},
};
fn scalar(name: &str) -> FieldDescriptorProto {
    FieldDescriptorProto {
        name: Some(name.into()),
        r#type: Some(Type::Int64 as i32),
        ..Default::default()
    }
}
fn message(name: &str, fields: Vec<FieldDescriptorProto>) -> DescriptorProto {
    DescriptorProto {
        name: Some(name.into()),
        field: fields,
        ..Default::default()
    }
}
fn child(name: &str, target: &str) -> FieldDescriptorProto {
    FieldDescriptorProto {
        name: Some(name.into()),
        r#type: Some(Type::Message as i32),
        type_name: Some(format!(".p.{target}")),
        label: Some(Label::Optional as i32),
        ..Default::default()
    }
}
fn schema(messages: Vec<DescriptorProto>) -> FileDescriptorSet {
    FileDescriptorSet {
        file: vec![FileDescriptorProto {
            package: Some("p".into()),
            message_type: messages,
            ..Default::default()
        }],
    }
}
fn large(name: &str, words: usize) -> DescriptorProto {
    message(name, (0..words).map(|i| scalar(&format!("s{i}"))).collect())
}
#[test]
fn threshold_and_minimum_saving_boundaries() {
    let fds = schema(vec![
        large("Big", 16),
        message("Parent", vec![child("big", "Big")]),
    ]);
    let opts = Options {
        field_threshold: 136,
        minimum_saving: 128,
        message_budget: 0,
    };
    assert!(select_with_options(&fds, opts).is_empty());
    let selected = select_with_options(
        &fds,
        Options {
            field_threshold: 135,
            ..opts
        },
    );
    assert_eq!(selected[0].path, ".p.Parent.big");
    assert!(selected[0].reason.contains("budget"));
    assert!(
        select_with_options(
            &fds,
            Options {
                field_threshold: 135,
                minimum_saving: 129,
                ..opts
            }
        )
        .is_empty()
    );
}
#[test]
fn child_first_avoids_redundant_parent_box_and_ties_are_stable() {
    let mut fds = schema(vec![
        large("Big", 32),
        message("Child", vec![child("z", "Big"), child("a", "Big")]),
        message("Parent", vec![child("child", "Child")]),
    ]);
    let opts = Options {
        field_threshold: 128,
        minimum_saving: 64,
        message_budget: 300,
    };
    let paths: Vec<_> = select_with_options(&fds, opts)
        .into_iter()
        .map(|s| s.path)
        .collect();
    assert_eq!(paths, vec![".p.Child.a"]);
    fds.file[0].message_type.reverse();
    fds.file[0].message_type[1].field.reverse();
    assert_eq!(
        paths,
        select_with_options(&fds, opts)
            .into_iter()
            .map(|s| s.path)
            .collect::<Vec<_>>()
    );
}
#[test]
fn tied_oneof_arms_require_joint_selection() {
    let mut host = message("Host", vec![child("a", "Big"), child("b", "Big")]);
    host.oneof_decl.push(OneofDescriptorProto {
        name: Some("value".into()),
        ..Default::default()
    });
    for f in &mut host.field {
        f.oneof_index = Some(0);
    }
    let fds = schema(vec![large("Big", 32), host]);
    assert_eq!(
        select_with_options(&fds, Options::default())
            .into_iter()
            .map(|s| s.path)
            .collect::<Vec<_>>(),
        vec![".p.Host.value.a", ".p.Host.value.b"]
    );
}

#[test]
fn recursive_and_repeated_fields_are_not_candidates() {
    let mut repeated = child("items", "Big");
    repeated.label = Some(Label::Repeated as i32);
    let fds = schema(vec![
        large("Big", 32),
        message("Node", vec![child("next", "Node"), repeated]),
        message("Left", vec![child("right", "Right")]),
        message("Right", vec![child("left", "Left")]),
    ]);
    assert!(
        select_with_options(
            &fds,
            Options {
                field_threshold: 0,
                minimum_saving: 0,
                message_budget: 0
            }
        )
        .is_empty()
    );
}

#[test]
fn synthetic_optional_uses_the_field_path() {
    let mut host = message("Host", vec![child("big", "Big")]);
    host.oneof_decl.push(OneofDescriptorProto {
        name: Some("_big".into()),
        ..Default::default()
    });
    host.field[0].oneof_index = Some(0);
    host.field[0].proto3_optional = Some(true);
    let fds = schema(vec![large("Big", 32), host]);
    let selected = select_with_options(
        &fds,
        Options {
            message_budget: 128,
            ..Options::default()
        },
    );
    assert_eq!(
        selected.into_iter().map(|s| s.path).collect::<Vec<_>>(),
        vec![".p.Host.big"]
    );
}

#[test]
fn map_entries_are_not_boxed() {
    let mut entry = message("Entry", vec![scalar("key"), child("value", "Big")]);
    entry.options = Some(MessageOptions {
        map_entry: Some(true),
        ..Default::default()
    });
    let mut entries = child("entries", "Entry");
    entries.label = Some(Label::Repeated as i32);
    let fds = schema(vec![
        large("Big", 32),
        entry,
        message("Host", vec![entries]),
    ]);
    assert!(
        select_with_options(
            &fds,
            Options {
                message_budget: 0,
                ..Options::default()
            }
        )
        .is_empty()
    );
}
