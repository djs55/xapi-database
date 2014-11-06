(*
 * Copyright (C) 2006-2009 Citrix Systems Inc.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU Lesser General Public License as published
 * by the Free Software Foundation; version 2.1 only. with the special
 * exception on linking described in file LICENSE.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Lesser General Public License for more details.
 *)

module Path = struct
  type t = string list

  let _xapi = "xapi"

  let root = [ _xapi ]

  let table tbl =  [ _xapi; tbl ]
  let obj tbl rf = [ _xapi; tbl; rf ]
  let field obj tbl rf field = [ _xapi; tbl; rf; field ]

end

open Irmin_unix
module Git = IrminGit.FS(struct
  let root = Some "/tmp/db.git"
  let bare = true
end)

module DB = Git.Make(IrminKey.SHA1)(IrminContents.String)(IrminTag.String)


let initialise () = ()

let get_table_from_ref dbref rf = None
	
let is_valid_ref dbref rf = false

let read_refs dbref tbl = []

let find_refs_with_filter dbref tbl expr = []

let read_field_where dbref where = []

let db_get_by_uuid dbref rbl uuid = failwith "unimplemented"

let db_get_by_name_label dbref tbl name_label = failwith "unimplemented"

let read_set_ref dbref where = []

let create_row dbref tbl kvpairs rf = ()

let delete_row dbref tbl rf = ()	

let write_field dbref tbl rf fld v = ()	

let read_field dbref tbl rf fld = failwith "unimplemented"

let read_record dbref tbl rf = failwith "unimplemented"

let read_records_where dbref tbl expr = []

let process_structured_field dbref kv tbl fld rf op = ()	
