{
  open Sql_parser
  let unquote x = String.sub x 1 (String.length x - 2)
}
rule token = parse
  | "SELECT"                           { SELECT }
  | "UPDATE"                           { UPDATE }
  | "INSERT"                           { INSERT }
  | "DELETE"                           { DELETE }
  | "WHERE"                            { WHERE }
  | "FROM"                             { FROM }
  | "SET"                              { SET }
  | "*"                                { STAR }
  | ['a'-'z''A'-'Z']['_''.''0'-'9''a'-'z''A'-'Z']* as x { IDENT x }
  | '\''([^'\'''\n']|'.')*'\'' as x    { STRING (unquote x) }
  | '"'([^'"''\n']|'.')*'"' as x       { STRING (unquote x) }
  | ','                                { COMMA }
  | '='                                { EQ }
  | [' ' '\t' '\n' ]                   { token lexbuf }
  | eof                                { EOF }
