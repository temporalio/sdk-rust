use crate::macro_utils::{ident_to_snake_case, method_name_to_pascal_case};
use proc_macro::TokenStream;
use proc_macro2::TokenStream as TokenStream2;
use quote::{format_ident, quote, quote_spanned};
use syn::{
    Attribute, FnArg, ItemTrait, ReturnType, TraitItem, Type,
    parse::{Parse, ParseStream},
    spanned::Spanned,
};

pub(crate) struct ActivityDefinitionsTrait {
    trait_item: ItemTrait,
    activities: Vec<ActivityDefMethod>,
}

#[derive(Default)]
struct ActivityAttributes {
    name_override: Option<syn::Expr>,
}

struct ActivityDefMethod {
    method: syn::TraitItemFn,
    attributes: ActivityAttributes,
    input_types: Vec<Type>,
    output_type: Option<Type>,
}

impl Parse for ActivityDefinitionsTrait {
    fn parse(input: ParseStream) -> syn::Result<Self> {
        let trait_item: ItemTrait = input.parse()?;
        let mut activities = Vec::new();
        for item in &trait_item.items {
            if let TraitItem::Fn(method) = item {
                activities.push(parse_activity_def_method(method)?);
            }
        }
        Ok(Self {
            trait_item,
            activities,
        })
    }
}

fn parse_activity_def_method(method: &syn::TraitItemFn) -> syn::Result<ActivityDefMethod> {
    if method.sig.asyncness.is_some() {
        return Err(syn::Error::new_spanned(
            &method.sig,
            "Activity definitions must not be async; declare only the input/output contract",
        ));
    }
    if matches!(method.sig.inputs.first(), Some(FnArg::Receiver(_))) {
        return Err(syn::Error::new_spanned(
            &method.sig,
            "Activity definitions must not take self; declare only the input/output contract",
        ));
    }
    Ok(ActivityDefMethod {
        method: method.clone(),
        attributes: extract_activity_attributes(&method.attrs)?,
        input_types: method
            .sig
            .inputs
            .iter()
            .filter_map(|arg| match arg {
                FnArg::Typed(p) => Some((*p.ty).clone()),
                FnArg::Receiver(_) => None,
            })
            .collect(),
        output_type: extract_output_type(&method.sig),
    })
}

fn extract_activity_attributes(attrs: &[Attribute]) -> syn::Result<ActivityAttributes> {
    let mut activity_attributes = ActivityAttributes::default();

    for attr in attrs {
        if attr.path().is_ident("activity") && attr.meta.require_list().is_ok() {
            attr.parse_nested_meta(|meta| {
                if meta.path.is_ident("name") {
                    let value = meta.value()?;
                    let expr: syn::Expr = value.parse()?;
                    activity_attributes.name_override = Some(expr);
                    Ok(())
                } else {
                    Err(meta.error("unsupported activity attribute"))
                }
            })?;
        }
    }

    Ok(activity_attributes)
}

fn extract_output_type(sig: &syn::Signature) -> Option<Type> {
    match &sig.output {
        ReturnType::Type(_, ty) => Some((**ty).clone()),
        ReturnType::Default => None,
    }
}

fn multi_args_input_type(types: &[Type]) -> TokenStream2 {
    match types.len() {
        0 => quote! { () },
        1 => {
            let t = &types[0];
            quote! { #t }
        }
        n => {
            let multi_args = format_ident!("MultiArgs{}", n);
            let types = types.iter();
            quote! { ::temporalio_workflow::common::data_converters::#multi_args<#(#types),*> }
        }
    }
}

impl ActivityDefinitionsTrait {
    pub(crate) fn codegen(&self) -> TokenStream {
        let trait_ident = &self.trait_item.ident;
        let module_ident = format_ident!("{}", ident_to_snake_case(&trait_ident.to_string()));
        let module_vis = &self.trait_item.vis;

        let mut cleaned_trait = self.trait_item.clone();
        for item in &mut cleaned_trait.items {
            if let TraitItem::Fn(method) = item {
                method
                    .attrs
                    .retain(|attr| !attr.path().is_ident("activity"));
            }
        }

        let marker_structs: Vec<_> = self
            .activities
            .iter()
            .map(|activity| {
                let struct_ident =
                    format_ident!("{}", method_name_to_pascal_case(&activity.method.sig.ident));
                quote! { pub struct #struct_ident; }
            })
            .collect();

        let marker_consts: Vec<_> = self
            .activities
            .iter()
            .map(|activity| {
                let method_ident = &activity.method.sig.ident;
                let struct_ident =
                    format_ident!("{}", method_name_to_pascal_case(&activity.method.sig.ident));
                let span = activity.method.span();
                quote_spanned! { span=>
                    #[allow(non_upper_case_globals, dead_code)]
                    pub const #method_ident: #struct_ident = #struct_ident;
                }
            })
            .collect();

        let activity_impls: Vec<_> = self
            .activities
            .iter()
            .map(|activity| {
                let method_ident = &activity.method.sig.ident;
                let struct_ident =
                    format_ident!("{}", method_name_to_pascal_case(&activity.method.sig.ident));
                let input_type = multi_args_input_type(&activity.input_types);
                let output_type = activity
                    .output_type
                    .as_ref()
                    .map(|t| quote! { #t })
                    .unwrap_or(quote! { () });
                let activity_name = if let Some(ref name_expr) = activity.attributes.name_override {
                    quote! { #name_expr }
                } else {
                    let default_name = format!("{}::{}", trait_ident, method_ident);
                    quote! { #default_name }
                };

                quote! {
                    impl #module_ident::#struct_ident {
                        /// Returns the activity name (delegates to ActivityDefinition::name()).
                        pub fn name(&self) -> &'static str {
                            <Self as ::temporalio_workflow::common::ActivityDefinition>::name()
                        }
                    }

                    impl ::temporalio_workflow::common::ActivityDefinition for #module_ident::#struct_ident {
                        type Input = #input_type;
                        type Output = #output_type;

                        fn name() -> &'static str
                        where
                            Self: Sized,
                        {
                            #activity_name
                        }
                    }
                }
            })
            .collect();

        quote! {
            #cleaned_trait

            #module_vis mod #module_ident {
                #(#marker_structs)*
                #(#marker_consts)*
            }

            #(#activity_impls)*
        }
        .into()
    }
}
