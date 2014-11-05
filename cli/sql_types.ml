open Sexplib.Std

type select = {
  columns: string list;
  tables: string list;
} with sexp

type update = {
  table: string;
  assignments: (string * string) list;
} with sexp

type expression =
| Select of select
| Update of update
with sexp

type result =
| Rows of string list list
| Unit
