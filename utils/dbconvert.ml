open Db_cache_types

let usage () =
  let name = Filename.basename Sys.executable_name in
  Printf.printf "Usage:\n%!";
  Printf.printf "./%s new-xml <db-path>\n%!" name;
  Printf.printf "./%s convert <db-path> <git-path>\n%!" name

let load_schema () =
  let sexp = Sexplib.Sexp.load_sexp "db.schema" in
  Schema.t_of_sexp sexp

let () =
  match Sys.argv with
  | [|_; "new-xml"; db_path|] ->
    let db = Database.make (load_schema ()) in
    Db_xml.To.file db_path db
  | [|_; "convert"; db_path; git_path|] ->
    let db = Db_xml.From.file (load_schema ()) db_path in
    (* create the 'relationship' fields *)
    let db = Database.reindex db in
    Lwt_main.run (Db_git.To.file git_path db)
  | _ -> usage ()
