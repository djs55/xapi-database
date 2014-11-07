open Lwt
open Irmin_unix
open Db_cache_types

let rec mkints first last =
  if first > last then []
  else first :: (mkints (first + 1) last)

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
            let pairs = Row.fold (fun k _ _ v acc ->
              let ty = try (Schema.Table.find k schema_table).Schema.Column.ty with _ -> Schema.Type.String in
              match ty with
              | Schema.Type.String -> ([k], Xml_spaces.protect v) :: acc
              | Schema.Type.Set ->
                let ks = String_unmarshall_helper.set (fun x -> x) v in
                let is = mkints 1 (List.length ks) in
                List.map (fun (i, k') -> [ k; string_of_int i ], k') (List.combine is ks) @ acc
              | Schema.Type.Pairs ->
                let pairs = String_unmarshall_helper.map (fun x -> x) (fun x -> x) v in
                List.map (fun (k', v) -> [ k; k' ], v) pairs @ acc
            ) row preamble in
            List.map (fun (k, v) -> "xapi" :: name :: rf :: k, v) pairs @ acc in
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
