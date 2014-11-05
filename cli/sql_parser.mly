%{
open Sql_types
%}
%token <string> IDENT STRING
%token SELECT INSERT UPDATE DELETE WHERE FROM SET COMMA EQ SEMICOLON EOF
%start expression             /* the entry point */
%type <Sql_types.expression> expression
%%
expression:
 | SELECT columns FROM tables   { Select { columns = $2; tables = $4 } }
 | UPDATE IDENT SET assignments { Update { table = $2; assignments = $4} }
;
columns:                       { [] }
 | IDENT                       { [ $1 ] }
 | IDENT COMMA columns         { $1::$3 }
;
tables:                        { [] }
 | IDENT                       { [ $1 ] }
 | IDENT COMMA tables          { $1::$3 }
;
assignments:                   { [] }
 | IDENT EQ IDENT              { [ $1, $3 ] }
 | IDENT EQ STRING             { [ $1, $3 ] }
 | IDENT EQ IDENT COMMA assignments { ($1, $3) :: $5 }
 | IDENT EQ STRING COMMA assignments { ($1, $3) :: $5 }
;
