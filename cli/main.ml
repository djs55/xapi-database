
let uri = ref (Uri.make ~scheme:"http" ~userinfo:"root:xenroot" ~host:"socket=/var/lib/xcp/xapi" ~path:"/remote_db_access" ())

module Http_client = XenAPI.Xen_api.Make(Xen_api_lwt_unix.Lwt_unix_IO)

module Remote_db : Db_interface.DB_ACCESS = Db_rpc_client_v1.Make(struct
  let t = ref (Http_client.make !uri)

  let initialise () = t := Http_client.make !uri

  let rpc request = match Lwt_main.run (Http_client.rpc ~timeout:5. !t request) with
  | XenAPI.Xen_api.Ok x -> Db_interface.String x
  | XenAPI.Xen_api.Error exn -> raise exn
end)

let _ =
  let all = Remote_db.read_refs Db_ref.Remote "VM" in
  List.iter (fun x -> output_string stdout x; output_string stdout "\n") all
