open OUnit

let dbref = Db_ref.Remote

let test_read_refs () =
  let refs = Db_git.Impl.read_refs dbref "pool" in
  assert_equal refs ["OpaqueRef:d2db1d86-9576-d595-33b9-1745dd532328"]

let test =
  "test_read" >:::
    [
      "test_read_refs" >:: test_read_refs;
    ]
