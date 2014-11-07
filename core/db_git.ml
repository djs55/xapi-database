open Lwt
open Irmin_unix

module To = struct
  let file (path: string) db : unit Lwt.t =
    let module Git = IrminGit.FS(struct
      let root = Some path
      let bare = true
    end) in
    let module Store = Git.Make(IrminKey.SHA1)(IrminContents.String)(IrminTag.String) in
    Store.create () >>=
      (fun store ->
        let open Db_cache_types in
        Store.View.of_path store []
        >>= fun view ->
        let table name _ _ (tbl: Table.t) acc =
          let record rf ctime mtime (row: Row.t) acc =
            let preamble =
              [("__mtime",Int64.to_string mtime); ("__ctime",Int64.to_string ctime); ("ref",rf)] in
            let pairs = Row.fold (fun k _ _ v acc -> (k, Xml_spaces.protect v) :: acc) row preamble in
            List.map (fun (k, v) -> [ "xapi"; name; rf; k ], v) pairs @ acc in
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
