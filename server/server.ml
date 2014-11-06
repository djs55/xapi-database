open Lwt

let f body =
 "hello"

module Request = Cohttp.Request.Make(Cohttp_lwt_unix_io) 
module Response = Cohttp.Response.Make(Cohttp_lwt_unix_io)

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
          let response_txt = f body in
          let content_length = String.length response_txt in
          let headers = Cohttp.Header.of_list [
            "user-agent", "xapi-database";
            "content-length", string_of_int content_length;
          ] in
          let response = Cohttp.Response.make
            ~version:`HTTP_1_1 ~status:`OK ~headers
            ~encoding:(Cohttp.Transfer.Fixed content_length) () in
          Response.write (fun t oc -> Response.write_body t oc response_txt) response oc
          >>= fun () ->
          loop ()
        end
      | _, _ -> fail (Failure "Unknown method")
      end in
  loop ()

let serve () =
  let s = Lwt_unix.socket Lwt_unix.PF_UNIX Lwt_unix.SOCK_STREAM 0 in
  Lwt_unix.bind s (Lwt_unix.ADDR_UNIX "/tmp/foo");
  Lwt_unix.listen s 5;
  let rec loop () =
    Lwt_unix.accept s
    >>= fun (fd, _) ->
    handle_fd fd;
    loop () in
  loop ()

let _ = Lwt_main.run (serve ())
