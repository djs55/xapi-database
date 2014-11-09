open Lwt

module type SERVER = sig
  val rpc: string option -> string -> string
end


module Request = Cohttp.Request.Make(Cohttp_lwt_unix_io) 
module Response = Cohttp.Response.Make(Cohttp_lwt_unix_io)

module Make(S: SERVER) = struct
  let handle_fd fd =
    let ic = Lwt_io.of_fd ~mode:Lwt_io.input fd in
    let oc = Lwt_io.of_fd ~mode:Lwt_io.output ~close:Lwt.return fd in
    let rec loop () =
      Request.read ic
      >>= function
      | `Eof -> fail (Failure "Failed to read HTTP request")
      | `Invalid x -> fail (Failure ("Failed to read HTTP request: " ^ x))
      | `Ok req ->
        begin match Cohttp.Request.meth req, Uri.path (Cohttp.Request.uri req) with
        | `POST, _ ->
          let headers = Cohttp.Request.headers req in
          begin match Cohttp.Header.get headers "content-length" with
          | None -> fail (Failure "Failed to read content-length")
          | Some content_length ->
            let content_length = int_of_string content_length in
            let body = String.create content_length in
            Lwt_io.read_into_exactly ic body 0 content_length
            >>= fun () ->
            let subtask_of = Cohttp.Header.get headers "subtask-of" in
            Printf.fprintf stderr "<- [%s] %s\n%!" (match subtask_of with None -> "None" | Some x -> x) body;
            let response_txt = S.rpc subtask_of body in
            Printf.fprintf stderr "-> %s\n%!" response_txt;
            let content_length = String.length response_txt in
            let headers = Cohttp.Header.of_list [
              "user-agent", "xapi-database";
              "content-length", string_of_int content_length;
            ] in
            let response = Cohttp.Response.make
              ~version:`HTTP_1_1 ~status:`OK ~headers
              ~encoding:(Cohttp.Transfer.Fixed content_length) () in
            Response.write (fun t oc -> Response.write_body t oc response_txt) response oc
          end
        | _, _ -> fail (Failure "Unknown method")
        end
        >>= fun () ->
        loop () in
    Lwt.catch loop
      (fun e ->
        Printf.fprintf stderr "* Caught %s\n%!" (Printexc.to_string e);
        Lwt_io.close ic
        >>= fun () ->
        Lwt_io.close oc
        >>= fun () ->
        fail e)


  let serve path =
    Lwt_unix.unlink path
    >>= fun () ->
    let s = Lwt_unix.socket Lwt_unix.PF_UNIX Lwt_unix.SOCK_STREAM 0 in
    Lwt_unix.bind s (Lwt_unix.ADDR_UNIX path);
    Lwt_unix.listen s 5;
    let rec loop () =
      Lwt_unix.accept s
      >>= fun (fd, _) ->
      handle_fd fd;
      loop () in
    loop ()
end

let _ =
  let module M = Make(Db_remote_cache_access_v1.Make(Db_git.Impl)) in

  Lwt_main.run (M.serve "/tmp/foo")
