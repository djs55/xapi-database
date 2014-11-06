open Lwt
open Irmin_unix

module To = struct
  let file (path: string) db : unit Lwt.t =
    let module Git = IrminGit.FS(struct
      let root = Some path
      let bare = false
    end) in
    let module Store = Git.Make(IrminKey.SHA1)(IrminContents.String)(IrminTag.String) in
    Store.create () >>=
      (fun store ->
        let open Db_cache_types in
        let table name _ _ (tbl: Table.t) acc =
          let record rf ctime mtime (row: Row.t) acc =
            let preamble =
              [("__mtime",Int64.to_string mtime); ("__ctime",Int64.to_string ctime); ("ref",rf)] in
            let pairs = Row.fold (fun k _ _ v acc -> (k, Xml_spaces.protect v) :: acc) row preamble in
            Lwt_list.iter_s
              (fun (k, v) ->
                Store.update store [ "xapi"; name; rf; k ] v
              ) pairs :: acc in
          Lwt.join (Table.fold record tbl []) :: acc in
        Lwt.join (TableSet.fold table (Database.tableset db) [])
      )
end
