use proc_macro::TokenStream;
use proc_macro2::Span;
use quote::quote;
use syn::{
    DataEnum, DataStruct, DeriveInput, Field, GenericArgument, Ident, Type, TypePath, TypeTuple,
    Variant, parse_macro_input,
};

#[derive(Debug, Clone)]
enum SQLField {
    Regular(Ident),
    Either(Ident, Ident),
    Option(Box<SQLField>),
    Vec(Box<SQLField>),
    Tuple(Box<Vec<SQLField>>),
    Variant(Box<Vec<(String, SQLField)>>),
}

extern crate proc_macro;

#[proc_macro_derive(SQLParser)]
pub fn derive_parser(inp: TokenStream) -> TokenStream {
    let inp = parse_macro_input!(inp as DeriveInput);
    println!("ident {:?}", inp.ident);
    match &inp.data {
        syn::Data::Struct(data_struct) => {
            //println!("{:?}", data_struct);
            let fields = derive_struct(&inp.ident, data_struct);
            println!("{:?}", fields);
        }
        syn::Data::Enum(data_enum) => {
            let fields = derive_enum(&inp.ident, data_enum);
            println!("{:?}", fields);
        }
        syn::Data::Union(data_union) => println!("{:?}", data_union),
    }
    let expanded = quote! {
        impl<'src, I, E> SQLParser<'src, I, E> for #inp.ident
        where
            I: Input<'src, Token = TokenStruct<'src>> + ValueInput<'src> + ExactSizeInput<'src>,
            E: ParserExtra<'src, I>,
            E::Error: LabelError<'src, I, String>,
        {
            fn parse(_args: ()) -> impl Parser<'src, I, Self, E> {
                custom(|stmt| {

                })
            }
        }
    };
    TokenStream::new()
}

fn generate_parser(field: &SQLField) -> String {
    let mut s = "".to_string();
    match field {
        SQLField::Regular(f) => if f == "StringLiteral" {
            
        },
        _ => {}
    }

    s
}

fn derive_enum(_id: &Ident, es: &DataEnum) -> Vec<(String, SQLField)> {
    //   println!("Enum : {:?}", es);
    let mut items = vec![];
    for v in &es.variants {
        items.push((v.ident.to_string(), handle_variant(v)));
    }
    items
}

fn handle_variant(v: &Variant) -> SQLField {
    let mut items = vec![];
    match &v.fields {
        syn::Fields::Named(n) => {
            for f in &n.named {
                items.push((f.ident.clone().unwrap().to_string(), handle_field(f)));
                //println!("named: {:?},{}", items.last().unwrap(), items.len());
            }
        }
        syn::Fields::Unnamed(u) => {
            for (i, f) in u.unnamed.iter().enumerate() {
                items.push((i.to_string(), handle_field(f)));
                // println!("unnamed : {:?},{}", items.last().unwrap(), items.len());
            }
        }
        syn::Fields::Unit => {
            panic!("unit field:{:?} ", v.ident)
        }
    }
    SQLField::Variant(Box::new(items))
}

fn derive_struct(_id: &Ident, st: &DataStruct) -> Vec<(String, SQLField)> {
    //we only handle named fields
    let mut fields = vec![];
    match &st.fields {
        syn::Fields::Named(fields_named) => {
            for f in &fields_named.named {
                let ident = f.ident.clone().unwrap();
                fields.push((ident.to_string(), handle_field(f)));
            }
        }
        syn::Fields::Unnamed(fields_unnamed) => {
            for (i, f) in fields_unnamed.unnamed.iter().enumerate() {
                fields.push((i.to_string(), handle_field(f)));
            }
        }
        syn::Fields::Unit => {
            panic!("Unit structs are not supported.")
        }
    }
    fields
}

fn handle_field(f: &Field) -> SQLField {
    match &f.ty {
        syn::Type::Path(type_path) => {
            // println!("here 4 : {:?}", type_path.path);
            let ty = type_path.path.segments[0].ident.clone();
            if ty == "Option" {
                handle_option(type_path)
            } else if ty == "Either" {
                handle_either(type_path)
            } else if ty == "Vec" {
                handle_vec(type_path)
            } else {
                SQLField::Regular(ty.clone())
            }
        }
        syn::Type::Tuple(tuple) => handle_tuple(tuple),
        _ => {
            panic!("Panic 2 : {:?}", f.ty);
        }
    }
}

fn handle_option(type_path: &TypePath) -> SQLField {
    //println!("handle options");
    let mut items = vec![];
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
                                    && let syn::PathArguments::AngleBracketed(_ag) = &pi.arguments
                                {
                                    items.push(handle_either(p));
                                }
                            }
                        } else {
                            items.push(SQLField::Regular(pi.clone().unwrap()))
                        }
                        //println!("Option with {:?},{:?}", pi, p);
                    } else if let syn::GenericArgument::Type(p) = a
                        && let syn::Type::Tuple(t) = p
                    {
                        items.push(handle_tuple(t));
                    } else {
                        panic!("Found this: {:?}", a);
                    }
                }
            }
            _ => {
                panic!("Panic 1");
            }
        }
    }
    assert!(items.len() == 1);
    SQLField::Option(Box::new(items[0].clone()))
}

fn handle_tuple(t: &TypeTuple) -> SQLField {
    let mut items = vec![];
    for ti in &t.elems {
        if let syn::Type::Path(p) = ti {
            //println!("Here 2");
            let it = p.path.get_ident().cloned().unwrap();
            //println!("Option tuple: {}", it);
            items.push(SQLField::Regular(it));
        }
    }
    SQLField::Tuple(Box::new(items))
}

fn handle_either(type_path: &TypePath) -> SQLField {
    //println!("handle either: {:?}", type_path);
    let mut items = vec![];
    for args in &type_path.path.segments {
        match &args.arguments {
            syn::PathArguments::AngleBracketed(angle_bracketed_generic_arguments) => {
                for ag in &angle_bracketed_generic_arguments.args {
                    if let syn::GenericArgument::Type(p) = ag
                        && let syn::Type::Path(p1) = p
                    {
                        let pi = p1.path.get_ident().cloned().unwrap();
                        //println!("Either : {:?}", pi);
                        items.push(pi);
                    } else {
                        panic!("Found this: {:?}", ag);
                    }
                }
            }
            syn::PathArguments::None => {
                //println!("none: {:?}", args.ident);
                items.push(args.ident.clone());
            }
            _ => {
                panic!("Either arguments are bad : {:?}", args);
            }
        }
    }
    assert!(items.len() == 2, "len = {},{:?}", items.len(), type_path);
    SQLField::Either(items[0].clone(), items[1].clone())
}

fn handle_vec(type_path: &TypePath) -> SQLField {
    let mut items = vec![];
    //println!("handling vec: {:?}", type_path);
    for args in &type_path.path.segments {
        if let syn::PathArguments::AngleBracketed(args) = &args.arguments {
            for a in &args.args {
                if let GenericArgument::Type(p) = &a
                    && let Type::Path(p) = &p
                {
                    //println!("Vec ags: {:?}", p.path.get_ident());
                    items.push(SQLField::Regular(p.path.get_ident().cloned().unwrap()));
                } else if let GenericArgument::Type(p) = &a
                    && let Type::Tuple(ty) = &p
                {
                    items.push(handle_tuple(ty));
                }
            }
        } else if let syn::PathArguments::Parenthesized(args) = &args.arguments {
            panic!("here : {:?}", args);
        }
    }
    assert_eq!(items.len(), 1);
    SQLField::Vec(Box::new(items[0].clone()))
}
