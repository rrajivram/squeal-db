//! `#[derive(SQLParser)]`: derive a token-level chumsky parser from the shape
//! of an AST type.
//!
//! A struct parses as the sequence of its fields; an enum parses as an
//! ordered choice of its variants (declaration order = priority, so put more
//! specific variants first). All per-field behavior comes from the field
//! type's own `SQLParser` impl — `Option<T>` is "maybe", `Vec<T>` is "zero or
//! more", `Either<L, R>` is "L else R", tuples are sequences (those blanket
//! impls live in `sql_parser::parser`). The derive itself only composes
//! `<Field as SQLParser>::parser(args.clone())` calls with `.then()` and maps
//! the nested tuples back into the type's constructor.
//!
//! Derived parsers take a `SqlCtx` as their args: the shared recursion
//! context holding the `Recursive` handles for `Expr` and `Query`, so that
//! mutually recursive grammar (subqueries inside expressions inside queries)
//! is built exactly once per knot instead of recursing at construction time.
//!
//! `#[sql_parser(body_only)]` generates the same parser as an inherent
//! `fn body_parser(args)` and skips the trait impl — used by the recursion
//! handle types themselves (`Query`), whose trait impl instead returns the
//! handle out of the context, while the knot-tier feeds `body_parser` into
//! `Recursive::define`.

use proc_macro::TokenStream;
use proc_macro2::TokenStream as TokenStream2;
use quote::{format_ident, quote};
use syn::{Data, DeriveInput, Fields, Ident, Type, parse_macro_input, spanned::Spanned};

extern crate proc_macro;

#[proc_macro_derive(SQLParser, attributes(sql_parser))]
pub fn derive_parser(inp: TokenStream) -> TokenStream {
    let inp = parse_macro_input!(inp as DeriveInput);
    let name = &inp.ident;

    if !inp.generics.params.is_empty() {
        return syn::Error::new(
            inp.generics.span(),
            "#[derive(SQLParser)] does not support generic types; write the impl by hand",
        )
        .to_compile_error()
        .into();
    }

    let body_only = inp.attrs.iter().any(|a| {
        a.path().is_ident("sql_parser")
            && a.parse_args::<Ident>()
                .map(|id| id == "body_only")
                .unwrap_or(false)
    });

    let (body, field_types) = match &inp.data {
        Data::Struct(data) => match fields_parser(&data.fields, quote!(Self)) {
            Ok(v) => v,
            Err(e) => return e.to_compile_error().into(),
        },
        Data::Enum(data) => {
            let mut variants = Vec::new();
            let mut types = Vec::new();
            for v in &data.variants {
                let vname = &v.ident;
                match fields_parser(&v.fields, quote!(Self::#vname)) {
                    Ok((expr, mut tys)) => {
                        variants.push(expr);
                        types.append(&mut tys);
                    }
                    Err(e) => return e.to_compile_error().into(),
                }
            }
            let Some(first) = variants.first().cloned() else {
                return syn::Error::new(name.span(), "cannot derive SQLParser for an empty enum")
                    .to_compile_error()
                    .into();
            };
            let rest = &variants[1..];
            (quote!( #first #( .or(#rest) )* ), types)
        }
        Data::Union(_) => {
            return syn::Error::new(name.span(), "cannot derive SQLParser for a union")
                .to_compile_error()
                .into();
        }
    };

    let bounds = field_types.iter().map(|ty| {
        quote!( #ty: ::sql_parser::parser::SQLParser<'src, I, E, ::sql_parser::parser::SqlCtx<'src, I, E>>, )
    });
    let where_clause = quote! {
        where
            I: ::sql_parser::parser::TokenInput<'src> + 'src,
            E: ::chumsky::extra::ParserExtra<'src, I> + 'src,
            E::Error: ::chumsky::label::LabelError<'src, I, ::std::string::String>,
            #(#bounds)*
    };

    let fn_body = quote! {
        {
            #[allow(unused_imports)]
            use ::chumsky::Parser as _;
            (#body).boxed()
        }
    };

    let expanded = if body_only {
        quote! {
            impl #name {
                /// The derived field-sequence parser, used by the recursion
                /// knot-tier (`SqlCtx::build`) to `define` this type's
                /// `Recursive` handle.
                pub fn body_parser<'src, I, E>(
                    args: ::sql_parser::parser::SqlCtx<'src, I, E>,
                ) -> impl ::chumsky::Parser<'src, I, Self, E> + Clone
                #where_clause
                #fn_body
            }
        }
    } else {
        quote! {
            impl<'src, I, E> ::sql_parser::parser::SQLParser<'src, I, E, ::sql_parser::parser::SqlCtx<'src, I, E>> for #name
            #where_clause
            {
                fn parser(
                    args: ::sql_parser::parser::SqlCtx<'src, I, E>,
                ) -> impl ::chumsky::Parser<'src, I, Self, E> + Clone
                #fn_body
            }
        }
    };
    expanded.into()
}

/// Build the parser expression for one struct body or enum variant:
/// sequence the field parsers with `.then()`, then map the nested tuple back
/// into `#constructor { .. }` / `#constructor(..)`. Returns the expression
/// and the field types (for where-clause bounds).
fn fields_parser(
    fields: &Fields,
    constructor: TokenStream2,
) -> syn::Result<(TokenStream2, Vec<Type>)> {
    let (types, names): (Vec<Type>, Vec<Option<Ident>>) = match fields {
        Fields::Named(named) => named
            .named
            .iter()
            .map(|f| (f.ty.clone(), f.ident.clone()))
            .unzip(),
        Fields::Unnamed(unnamed) => unnamed
            .unnamed
            .iter()
            .map(|f| (f.ty.clone(), None))
            .unzip(),
        Fields::Unit => {
            return Err(syn::Error::new(
                constructor.span(),
                "SQLParser cannot be derived for unit types: there is nothing to parse",
            ));
        }
    };

    if types.is_empty() {
        return Err(syn::Error::new(
            constructor.span(),
            "SQLParser cannot be derived for empty field lists: there is nothing to parse",
        ));
    }

    let binders: Vec<Ident> = (0..types.len()).map(|i| format_ident!("f{i}")).collect();

    // f0, (f0, f1), ((f0, f1), f2), ... — the tuple shape `.then()` chains
    // produce.
    let first_binder = &binders[0];
    let mut pattern = quote!(#first_binder);
    for b in &binders[1..] {
        pattern = quote!((#pattern, #b));
    }

    let first_ty = &types[0];
    let rest_ty = &types[1..];
    let chain = quote! {
        <#first_ty as ::sql_parser::parser::SQLParser<'src, I, E, ::sql_parser::parser::SqlCtx<'src, I, E>>>::parser(args.clone())
        #( .then(<#rest_ty as ::sql_parser::parser::SQLParser<'src, I, E, ::sql_parser::parser::SqlCtx<'src, I, E>>>::parser(args.clone())) )*
    };

    let build = match fields {
        Fields::Named(_) => {
            let names = names.into_iter().map(|n| n.unwrap());
            quote!( #constructor { #( #names: #binders ),* } )
        }
        Fields::Unnamed(_) => quote!( #constructor ( #( #binders ),* ) ),
        Fields::Unit => unreachable!(),
    };

    Ok((quote!( #chain.map(|#pattern| #build) ), types))
}
