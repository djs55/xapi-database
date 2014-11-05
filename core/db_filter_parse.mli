type token =
  | EQ
  | TRUE
  | FALSE
  | FIELD
  | LAND
  | LOR
  | LNOT
  | LPAREN
  | RPAREN
  | EOF
  | IDENT of (string)

val exprstr :
  (Lexing.lexbuf  -> token) -> Lexing.lexbuf -> Db_filter_types.expr
