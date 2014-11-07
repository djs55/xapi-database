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

open Lwt
open Irmin_unix
open Db_cache_types
open Db_exn

module Path = struct
  type t = string list

  let _xapi = "xapi"

  let _ref_to_table = "tables"

  let root = [ _xapi ]

  let table tbl =  [ _xapi; tbl ]
  let obj tbl rf = [ _xapi; tbl; rf ]
  let field tbl rf field = [ _xapi; tbl; rf; field ]
  let set tbl rf field key = [ _xapi; tbl; rf; field; key ]

  let ref_to_table rf = [ _xapi; _ref_to_table; rf ]
end

let marshal_pairs k v =
  let pairs = String_unmarshall_helper.map (fun x -> x) (fun x -> x) v in
  List.map (fun (k', v) -> k @ [ k' ], v) pairs

let marshal_set k v =
  let set = String_unmarshall_helper.set (fun x -> x) v in
  List.map (fun k' -> k @ [ "_" ^ k' ], "1") set
       
module To = struct
  let file (path: string) db : unit Lwt.t =
    let module Git = IrminGit.FS(struct
      let root = Some path
      let bare = true
    end) in
    let module Store = Git.Make(IrminKey.SHA1)(IrminContents.String)(IrminTag.String) in
    let schema = Database.schema db in
    let schema_db = schema.Schema.database in

    Store.create () >>=
      (fun store ->
        let open Db_cache_types in
        Store.View.of_path store []
        >>= fun view ->
        let table name _ _ (tbl: Table.t) acc =
          let schema_table = Schema.Database.find name schema_db in
          let record rf ctime mtime (row: Row.t) acc =
            let preamble =
              [(["__mtime"],Int64.to_string mtime); (["__ctime"],Int64.to_string ctime); (["ref"],rf)] in
            let index = [ Path.ref_to_table rf, name ] in
            let pairs = Row.fold (fun k _ _ v acc ->
              let ty = try (Schema.Table.find k schema_table).Schema.Column.ty with _ -> Schema.Type.String in
              match ty with
              | Schema.Type.String -> ([k], Xml_spaces.protect v) :: acc
              | Schema.Type.Set -> marshal_set [k] v @ acc
              | Schema.Type.Pairs -> marshal_pairs [k] v @ acc
            ) row preamble in
            List.map (fun (k, v) -> Path._xapi :: name :: rf :: k, v) pairs @ index @ acc in
          Table.fold record tbl [] @ acc in
        let pairs = TableSet.fold table (Database.tableset db) [] in
        Lwt_list.iter_s (fun (k, v) -> Store.View.update view k v) pairs
        >>= fun () ->
        let origin = IrminOrigin.create "Initial import of %s" path in
        Store.View.merge_path ~origin store [] view
        >>= function
        | `Ok () -> return ()
        | `Conflict msg -> failwith (Printf.sprintf "Conflict during final merge: %s" msg)
      )
end

let schema =
  let sexp = Sexplib.Sexp.load_sexp "db.schema" in
  Schema.t_of_sexp sexp

module Git = IrminGit.FS(struct
  let root = Some "/tmp/db"
  let bare = true
end)

module Store = Git.Make(IrminKey.SHA1)(IrminContents.String)(IrminTag.String)

let store_t = Store.create ()

let read path =
  store_t >>= fun store ->
  Store.read store path

let write path v =
  store_t >>= fun store ->
  Store.update store path v

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

let rm path =
  store_t >>= fun store ->
  Store.list store [ path ] >>= fun keys ->
  Lwt_list.iter_s (Store.remove store) keys

let filter_none xs = List.fold_left (fun acc x -> match x with None -> acc | Some x -> x :: acc) [] xs

let read_set dbref tbl rf fld =
  ls (Path.field tbl rf fld)
  >>= fun keys ->
  Lwt_list.map_s (fun key -> read(Path.set tbl rf fld key)) keys
  >>= fun values ->
  return (filter_none values)

let read_map dbref tbl rf fld =
  ls (Path.field tbl rf fld)
  >>= fun keys ->
  Lwt_list.map_s (fun key ->
    read (Path.set tbl rf fld key)
    >>= function
    | None -> return (key, "")
    | Some v -> return (key, v)
  ) keys

open Db_cache_types
open Db_exn

module Impl = struct
  let initialise () = ()

  let get_table_from_ref dbref rf =
    Lwt_main.run (read (Path.ref_to_table rf))

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

  let read_set_ref = read_field_where

  let db_get_by_uuid dbref tbl uuid_val =
    let where = { table=tbl; return=Db_names.ref; where_field=Db_names.uuid; where_value=uuid_val } in
    match read_field_where dbref where with
    | [] -> raise (Read_missing_uuid (tbl, "", uuid_val))
    | [r] -> r
    | _ -> raise (Too_many_values (tbl, "", uuid_val))

  let db_get_by_name_label dbref tbl label =
    let where = { table=tbl; return=Db_names.ref; where_field=(Escaping.escape_id ["name"; "label"]); where_value = label } in
    read_field_where dbref where

  let delete_row dbref tbl rf =
    Lwt_main.run (rm (Path.obj tbl rf));
    Lwt_main.run (rm (Path.ref_to_table rf))

  let write_field dbref tbl rf fld v =
    Lwt_main.run (write (Path.field tbl rf fld) v)

  let read_field dbref tbl rf fld =
    match Lwt_main.run (read (Path.field tbl rf fld)) with
    | None -> raise (DBCache_NotFound(tbl,rf,fld))
    | Some x -> x

  let read_record dbref tbl rf =
    let t =
      ls (Path.obj tbl rf)
      >>= fun fields ->
      let schema = Schema.Database.find tbl schema.Schema.database in
      let set_ref = List.filter (fun field -> Schema.((Table.find field schema).Column.issetref)) fields in
      let sets = List.filter (fun field -> Schema.((Table.find field schema).Column.ty = Type.Set)) fields in
      let maps = List.filter (fun field -> Schema.((Table.find field schema).Column.ty = Type.Pairs)) fields in
      let strings = List.filter (fun field -> Schema.((Table.find field schema).Column.ty = Type.String)) fields in
      Lwt_list.map_s (fun field ->
        read (Path.field tbl rf field)
        >>= function
        | None -> return (field, "")
        | Some x -> return (field, x)
      ) strings
      >>= fun strings ->
      (* return sets and maps as s-expressions *)
      Lwt_list.map_s (fun field -> 
        read_set dbref tbl rf field
        >>= fun values ->
        return (field, String_marshall_helper.set (fun x -> x) values)
      ) sets
      >>= fun sets ->
      Lwt_list.map_s (fun field ->
        read_map dbref tbl rf field
        >>= fun values ->
        return (field, String_marshall_helper.map (fun x -> x) (fun x -> x) values)
      ) maps
      >>= fun maps ->
      Lwt_list.map_s (fun field ->
        read_set dbref tbl rf field
        >>= fun values ->
        return (field, values)
      ) set_ref
      >>= fun set_ref ->
      return (strings @ sets @ maps, set_ref) in
    Lwt_main.run t

  let read_records_where dbref tbl expr =
    let reqd_refs = find_refs_with_filter dbref tbl expr in
    List.map (fun ref->ref, read_record dbref tbl ref) reqd_refs

  let create_row dbref tbl kvpairs rf =
    let schema = Schema.Database.find tbl schema.Schema.database in
    let set_ref = List.filter (fun (field,_) -> Schema.((Table.find field schema).Column.issetref)) kvpairs in
    let to_write = List.concat (List.map (fun (k, v) ->
      let open Schema in
      let path = Path.field tbl rf k in
      match (Table.find k schema).Column.ty with
      | Type.String -> [ path, v ]
      | Type.Set -> marshal_set path v
      | Type.Pairs -> marshal_pairs path v
    ) kvpairs) in
    let preamble = [ Path.ref_to_table rf, tbl ] in
    Lwt_main.run (Lwt_list.iter_s (fun (k, v) -> write k v) (to_write @ preamble))

  let process_structured_field dbref (key,value) tbl fld rf op =
    (* Ensure that both keys and values are valid for UTF-8-encoded XML. *)
    let open String_marshall_helper in
    let key = ensure_utf8_xml key in
    let value = ensure_utf8_xml value in
    let t = match op with
    | AddMap ->
      begin
        read (Path.set tbl rf fld key)
        >>= function
        | Some _ -> fail (Duplicate_key (tbl,fld,rf,key))
        | None -> write (Path.set tbl rf fld key) value
      end
    | AddSet -> write (Path.set tbl rf fld key) "1"
    | RemoveMap -> rm (Path.set tbl rf fld key)
    | RemoveSet -> rm (Path.set tbl rf fld ("_" ^ key)) in
    Lwt_main.run t
end
