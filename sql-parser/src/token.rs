use crate::{keyword::Keyword, span::TokenSpan};

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum Token<'src> {
    /// An unsigned numeric literal (`123`, `1.5`, `.5`). Sign is handled by
    /// the expression parser as a unary operator; the raw text is kept so the
    /// literal parser can decide between integer and float.
    Number {
        raw: &'src str,
    },
    Word {
        raw: &'src str,
        keyword: Option<Keyword>,
    },
    String {
        raw: &'src str,
        kind: StringStyle,
    },
    Space,
    /// A multi-character operator (`<=`, `!=`, `||`, ...). Lexed as one token
    /// so the parser never has to ask whether two punctuation marks were
    /// adjacent in the source.
    Operator(Operator),
    Punctuation(Punctuation),
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct TokenStruct<'src> {
    pub token: Token<'src>,
    pub span: TokenSpan,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum StringStyle {
    SingleQuoted(Option<char>),
    DoubleQuoted(Option<char>),
    Unquoted,
}

impl<'src> Token<'src> {
    pub fn is_whitespace(c: char) -> bool {
        matches!(c, ' ' | '\r' | '\n' | '\t')
    }
}

impl<'src> std::fmt::Display for Token<'src> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", String::from(self.clone()))
    }
}

impl<'src> std::fmt::Display for TokenStruct<'src> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.token)
    }
}

impl<'src> From<Token<'src>> for String {
    fn from(value: Token<'src>) -> Self {
        match value {
            Token::Number { raw } => String::from(raw),
            Token::Word { raw, keyword: _ } => String::from(raw),
            Token::String { raw, kind } => match kind {
                StringStyle::SingleQuoted(c) | StringStyle::DoubleQuoted(c) if c.is_some() => {
                    let c = c.unwrap();
                    format!("{c}{raw}{c}")
                }
                _ => String::from(raw),
            },
            Token::Space => String::from(" "),
            Token::Operator(op) => String::from(op.as_str()),
            Token::Punctuation(punctuation) => String::from(punctuation.to_char()),
        }
    }
}

macro_rules! create_operators {
    ($(($s:literal,$i:ident)),*$(,)?) => {
        #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
        pub enum Operator {
            $($i,)*
        }

        impl Operator {
            pub fn as_str(self) -> &'static str {
                match self {
                    $(Self::$i => $s,)*
                }
            }

            /// All operators, longest spelling first, for greedy lexing.
            pub const ALL: &'static [(&'static str, Operator)] = &[
                $(($s, Operator::$i),)*
            ];
        }
    };
}

// Order matters: the lexer tries these top to bottom, so longer operators
// sharing a prefix with shorter ones must come first.
create_operators!(
    ("<>", NotEq),
    ("!=", NotEqBang),
    ("<=", LtEq),
    (">=", GtEq),
    ("||", Concat),
    ("::", DoubleColon),
);

macro_rules! create_punctuation {
    ($(($x:expr,$c:literal,$i:ident)),*$(,)?) => {
        #[derive(Debug, Clone, Copy, PartialEq, Eq,Hash)]
        pub enum Punctuation {
            $($i,)*
        }

        impl Punctuation {
            pub fn from_char(c: char) -> Option<Self> {
                match c {
                    $($c => Some(Self::$i),)*
                    _ => None,
                }
            }

            pub fn to_char(self) -> char {
                match self {
                    $(Self::$i => $c,)*
                }
            }
        }

        // One span-carrying struct per punctuation variant, mirroring the
        // keyword structs: a parser can match a specific punctuation mark and
        // keep the matched token's span in a distinctly-typed value.
        $(
            #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
            pub struct $i {
                pub span: crate::span::TokenSpan,
            }

            impl $i {
                pub fn new(span: crate::span::TokenSpan) -> Self {
                    Self { span }
                }
            }

            // Generic over the args type `A`, like the keyword impls.
            impl<'src, I, E, A> crate::parser::SQLParser<'src, I, E, A> for $i
            where
                I: chumsky::input::Input<'src, Token = TokenStruct<'src>>
                    + chumsky::input::ValueInput<'src>
                    + chumsky::input::ExactSizeInput<'src>,
                E: chumsky::extra::ParserExtra<'src, I>,
                E::Error: chumsky::label::LabelError<'src, I, ::std::string::String>,
            {
                fn parser(_args: A) -> impl chumsky::Parser<'src, I, Self, E> + Clone {
                    use chumsky::Parser;
                    chumsky::prelude::any()
                        .try_map(|t: TokenStruct<'src>, span| match t.token {
                            Token::Punctuation(Punctuation::$i) => Ok($i { span: t.span }),
                            _ => Err(chumsky::label::LabelError::<'src, I, ::std::string::String>::expected_found(
                                [::std::string::String::from($c)],
                                Some(chumsky::util::MaybeRef::Val(t)),
                                span,
                            )),
                        })
                }
            }
        )*
    };
}

create_punctuation!(
    (0x21, '!', ExclamationMark),
    (0x23, '#', NumberSign),
    (0x24, '$', Dollar),
    (0x25, '%', Percent),
    (0x26, '&', Ampersand),
    (0x28, '(', LeftParenthesis),
    (0x29, ')', RightParenthesis),
    (0x2A, '*', Asterisk),
    (0x2B, '+', Plus),
    (0x2C, ',', Comma),
    (0x2D, '-', Minus),
    (0x2E, '.', Period),
    (0x2F, '/', Slash),
    (0x3A, ':', Colon),
    (0x3B, ';', Semicolon),
    (0x3C, '<', LessThan),
    (0x3D, '=', Equals),
    (0x3E, '>', GreaterThan),
    (0x3F, '?', QuestionMark),
    (0x40, '@', At),
    (0x5B, '[', LeftBracket),
    (0x5C, '\\', Backslash),
    (0x5D, ']', RightBracket),
    (0x5E, '^', Caret),
    (0x7B, '{', LeftBrace),
    (0x7C, '|', VerticalBar),
    (0x7D, '}', RightBrace),
    (0x7E, '~', Tilde),
);
