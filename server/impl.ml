(*
 * Copyright (C) 2006-2009 Citrix Systems Inc.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU Lesser General Public License as published
 * by the Free Software Foundation; version 2.1 only. with the special
 * exception on linking described in file LICENSE.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Lesser General Public License for more details.
 *)

module Path = struct
  type t = string list

  let _xapi = "xapi"

  let _ref_to_table = "tables"

  let root = [ _xapi ]

  let table tbl =  [ _xapi; tbl ]
  let obj tbl rf = [ _xapi; tbl; rf ]
  let field tbl rf field = [ _xapi; tbl; rf; field ]

end

open Irmin_unix
module Git = IrminGit.FS(struct
  let root = Some "/tmp/db"
  let bare = true
end)

module Store = Git.Make(IrminKey.SHA1)(IrminContents.String)(IrminTag.String)

let store_t = Store.create ()

open Lwt

let read path =
  store_t >>= fun store ->
  Store.read store path

let rec remove_prefix prefix path = match prefix, path with
| [], path -> path
| p :: ps, q :: qs when p = q -> remove_prefix ps qs
| _, _ -> failwith (Printf.sprintf "%s is not a prefix of %s" (String.concat "/" prefix) (String.concat "/" path))

let union xs x = if not(List.mem x xs) then x :: xs else xs

let setify xs = List.fold_left union [] xs

let ls (path: string list) : string list Lwt.t =
  store_t >>= fun store ->
  Store.list store [ path ] >>= fun keys ->
  let children = setify (List.map (fun key -> List.hd (remove_prefix path key)) keys) in
  return children

open Db_cache_types
open Db_exn

let initialise () = ()

let get_table_from_ref dbref rf =
  Lwt_main.run (read [ Path._xapi; Path._ref_to_table; rf ])
	
let is_valid_ref dbref rf = match get_table_from_ref dbref rf with
  | None -> false
  | Some _ -> true

let read_refs dbref tbl =
  Lwt_main.run (ls [ Path._xapi; tbl ])

let find_refs_with_filter dbref tbl expr =
  let eval_row rf = function
  | Db_filter_types.Literal x -> x
  | Db_filter_types.Field x ->
    begin match Lwt_main.run (read (Path.field tbl rf x)) with
    | Some x -> x
    | None -> failwith ("Couldn't find field " ^ x)
    end in
  let rfs = read_refs dbref tbl in
  List.fold_left (fun acc rf ->
    if Db_filter.eval_expr (eval_row rf) expr
    then rf :: acc
    else acc
  ) rfs []

let read_field_where dbref where =
  let rfs = read_refs dbref where.table in
  List.fold_left (fun acc rf ->
    match Lwt_main.run (read (Path.field where.table rf where.where_field)) with
    | Some v when v = where.where_value ->
      begin match Lwt_main.run (read (Path.field where.table rf where.return)) with
      | Some w -> w :: acc
      | None -> acc
      end
    | _ -> acc) [] rfs

let db_get_by_uuid dbref tbl uuid_val =
  let where = { table=tbl; return=Db_names.ref; where_field=Db_names.uuid; where_value=uuid_val } in
  match read_field_where dbref where with
  | [] -> raise (Read_missing_uuid (tbl, "", uuid_val))
  | [r] -> r
  | _ -> raise (Too_many_values (tbl, "", uuid_val))

let db_get_by_name_label dbref tbl label =
  let where = { table=tbl; return=Db_names.ref; where_field=(Escaping.escape_id ["name"; "label"]); where_value = label } in
  read_field_where dbref where

let read_set_ref dbref where = []

let create_row dbref tbl kvpairs rf = ()

let delete_row dbref tbl rf = ()	

let write_field dbref tbl rf fld v = ()	

let read_field dbref tbl rf fld = failwith "unimplemented"

let read_record dbref tbl rf = failwith "unimplemented"

let read_records_where dbref tbl expr = []

let process_structured_field dbref kv tbl fld rf op = ()	
