
let uri = ref (Uri.make ~scheme:"http" ~userinfo:"root:xenroot" ~host:"socket=/var/lib/xcp/xapi" ~path:"/remote_db_access" ())

module Http_client = XenAPI.Xen_api.Make(Xen_api_lwt_unix.Lwt_unix_IO)

module Remote_db : Db_interface.DB_ACCESS = Db_rpc_client_v1.Make(struct
  let t = ref (Http_client.make !uri)

  let initialise () = t := Http_client.make !uri

  let rpc request = match Lwt_main.run (Http_client.rpc ~timeout:5. !t request) with
  | XenAPI.Xen_api.Ok x -> Db_interface.String x
  | XenAPI.Xen_api.Error exn -> raise exn
end)

let query = "SELECT VM.name__label FROM VM"

open Sql_types

let filter columns (x, _) = List.mem "" columns || (List.mem x columns)

let run query =
  let lexbuf = Lexing.from_string query in
  match Sql_parser.expression Sql_lexer.token lexbuf with
  | Select { tables; columns } ->
    let tables = List.map (fun table ->
      let rows = Remote_db.read_records_where Db_ref.Remote table Db_filter_types.True in
      (* convert to pairs with the table name prefixed *)
      List.map (fun (_, (pairs, _)) ->
        List.map (fun (k, v) -> table ^ "." ^ k, v) pairs
      ) rows
    ) tables in
    (* this is where a cross product would go *)
    let table = List.concat tables in
    (* this is where a filter would go *)
    (* this is where a projection would go *)
    let table = List.map (List.filter (filter columns)) table in
    List.iter (fun pairs ->
      List.iter (fun (k, v) -> Printf.printf "%s = %s\n" k v) pairs
    ) table

  | _ -> failwith "unimplemented"

let _ =
(*
  let all = Remote_db.read_refs Db_ref.Remote "VM" in
  List.iter (fun x -> output_string stdout x; output_string stdout "\n") all;
*)
(*
  let lexbuf = Lexing.from_channel stdin in
  let exp = Sql_parser.expression Sql_lexer.token lexbuf in
  output_string stdout (Sexplib.Sexp.to_string_hum (Sql_types.sexp_of_expression exp));
  ()
*)
  run query
