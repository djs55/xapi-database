open OUnit

let xml_path = "test/data/state.db"
let git_path = "/tmp/db"

let load_schema () =
  let sexp = Sexplib.Sexp.load_sexp "db.schema" in
  Schema.t_of_sexp sexp

let setup () =
  let db = Db_xml.From.file (load_schema ()) xml_path in
  Lwt_main.run (Db_git.To.file git_path db)

let base_suite =
  "base_suite" >:::
    [
      Test_read.test;
    ]

let _ =
  setup ();
  run_test_tt ~verbose:false base_suite
