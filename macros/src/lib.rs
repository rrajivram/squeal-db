use proc_macro::TokenStream;
use syn::{
    AngleBracketedGenericArguments, DataEnum, DataStruct, DeriveInput, Field, GenericArgument,
    Ident, Type, TypePath, TypeTuple, Variant, parse_macro_input, token::Struct,
};

extern crate proc_macro;

#[proc_macro_derive(SQLParser)]
pub fn derive_parser(inp: TokenStream) -> TokenStream {
    let inp = parse_macro_input!(inp as DeriveInput);
    println!("ident {:?}", inp.ident);
    match inp.data {
        syn::Data::Struct(data_struct) => {
            //println!("{:?}", data_struct);
            derive_struct(&inp.ident, &data_struct);
        }
        syn::Data::Enum(data_enum) => {
            derive_enum(&inp.ident, &data_enum);
        }
        syn::Data::Union(data_union) => println!("{:?}", data_union),
    }
    TokenStream::new()
}

fn derive_enum(id: &Ident, es: &DataEnum) {
    println!("Enum : {:?}", es);
    for v in &es.variants {
        handle_variant(v);
    }
}

fn handle_variant(v: &Variant) {
    match &v.fields {
        syn::Fields::Named(n) => {
            for f in &n.named {
                handle_named_field(&f.ident.clone().unwrap(), f);
            }
        }
        syn::Fields::Unnamed(u) => {
            println!("Unnamed : {:?}", u);
        }
        syn::Fields::Unit => {
            println!("unit field:{:?} ", v.ident)
        }
    }
}

fn derive_struct(id: &Ident, st: &DataStruct) {
    //we only handle named fields
    match &st.fields {
        syn::Fields::Named(fields_named) => {
            for f in &fields_named.named {
                let ident = f.ident.clone().unwrap();
                println!("name : {}", ident);
                handle_named_field(&ident, f);
            }
        }
        syn::Fields::Unnamed(_fields_unnamed) => {
            panic!("Unnamed fields are not supported.");
        }
        syn::Fields::Unit => {
            panic!("Unit structs are not supported.")
        }
    }
}

fn handle_named_field(id: &Ident, f: &Field) {
    match &f.ty {
        syn::Type::Path(type_path) => {
            println!("here 4 : {:?}", type_path.path);
            let ty = type_path.path.segments[0].ident.clone();
            if ty == "Option" {
                handle_option(type_path);
            } else if ty == "Either" {
                handle_either(type_path);
            } else if ty == "Vec" {
                handle_vec(type_path);
            }
        }
        syn::Type::Tuple(tuple) => {
            handle_tuple(tuple);
        }
        _ => {
            panic!("Panic 2 : {:?}", f.ty);
        }
    }
}

fn handle_keyword() {}

fn handle_option(type_path: &TypePath) {
    println!("handle options");
    for s in &type_path.path.segments {
        match &s.arguments {
            syn::PathArguments::AngleBracketed(angle_bracketed_generic_arguments) => {
                for a in &angle_bracketed_generic_arguments.args {
                    if let syn::GenericArgument::Type(p) = a
                        && let syn::Type::Path(p) = p
                    {
                        let pi = p.path.get_ident().cloned();
                        if pi.is_none() {
                            for pi in &p.path.segments {
                                if pi.ident == "Either"
                                    && let syn::PathArguments::AngleBracketed(ag) = &pi.arguments
                                {
                                    for args in &ag.args {
                                        if let syn::GenericArgument::Type(ty) = &args
                                            && let syn::Type::Path(p) = &ty
                                        {
                                            handle_either(p);
                                        }
                                    }
                                }
                            }
                        }
                        println!("Option with {:?},{:?}", pi, p);
                    } else if let syn::GenericArgument::Type(p) = a
                        && let syn::Type::Tuple(t) = p
                    {
                        handle_tuple(t);
                    } else {
                        println!("Found this: {:?}", a);
                    }
                }
            }
            _ => {
                panic!("Panic 1");
            }
        }
    }
}

fn handle_tuple(t: &TypeTuple) {
    for ti in &t.elems {
        if let syn::Type::Path(p) = ti {
            println!("Here 2");
            let it = p.path.get_ident().cloned().unwrap();
            println!("Option tuple: {}", it);
        }
    }
}

fn handle_either(type_path: &TypePath) {
    println!("handle either: {:?}", type_path);
    for args in &type_path.path.segments {
        match &args.arguments {
            syn::PathArguments::AngleBracketed(angle_bracketed_generic_arguments) => {
                for ag in &angle_bracketed_generic_arguments.args {
                    if let syn::GenericArgument::Type(p) = ag
                        && let syn::Type::Path(p1) = p
                    {
                        let pi = p1.path.get_ident().cloned().unwrap();
                        println!("Either : {:?}", pi);
                    } else {
                        println!("Found this: {:?}", ag);
                    }
                }
            }
            syn::PathArguments::None => {
                println!("{:?}", args.ident);
            }
            _ => {
                panic!("Either arguments are bad : {:?}", args);
            }
        }
    }
}

fn handle_vec(type_path: &TypePath) {
    println!("handling vec: {:?}", type_path);
    for args in &type_path.path.segments {
        if let syn::PathArguments::AngleBracketed(args) = &args.arguments {
            for a in &args.args {
                if let GenericArgument::Type(p) = &a
                    && let Type::Path(p) = &p
                {
                    println!("Vec ags: {:?}", p.path.get_ident());
                } else if let GenericArgument::Type(p) = &a
                    && let Type::Tuple(ty) = &p
                {
                    handle_tuple(ty);
                }
            }
        } else if let syn::PathArguments::Parenthesized(args) = &args.arguments {
            println!("here : {:?}", args);
        }
    }
}
