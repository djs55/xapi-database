(*
 * Copyright (C) 2010 Citrix Systems Inc.
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


module Tests = functor(Client: Db_interface.DB_ACCESS) -> struct

	let name = "thevmname"
	let invalid_name = "notavmname"
		
	let make_vm r uuid = 
		[ 
			"uuid", uuid;
			"name__description", "";
			"other_config", "()";
			"tags", "()";
			"name__label", name;
		]
			
	let make_vbd vm r uuid = [
(*		"ref", r; *)
		"qos__supported_algorithms", "()";
		"other_config", "(('owner' ''))";
		"uuid", uuid;
		"allowed_operations", "('attach')";
		"qos__algorithm_params", "()";
		"type", "Disk";
		"VM", vm;
		"VDI", "OpaqueRef:NULL";
		"qos__algorithm_type", "";
		"metrics", "OpaqueRef:NULL";
		"device", "";
		"empty", "false";
		"bootable", "false";
		"current_operations", "()";
		"unpluggable", "true";
		"status_detail", "";
		"runtime_properties", "()";
		"userdevice", "0";
		"mode", "RW";
		"storage_lock", "false";
		"status_code", "0";
		"currently_attached", "false";
	]
		
	let expect_missing_row tbl r f = 
		try
			f ()
		with Db_exn.DBCache_NotFound("missing row", tbl', r') when tbl' = tbl && r = r' -> ()

	let expect_missing_tbl tbl f = 
		try
			f ()
		with Db_exn.DBCache_NotFound("missing table", tbl', "") when tbl' = tbl -> ()
			
	let expect_uniqueness_violation tbl fld v f = 
		try
			f ()
		with Db_exn.Uniqueness_constraint_violation(tbl', fld', v') when tbl' = tbl && fld' = fld && v' = v -> ()
	
	let expect_missing_uuid tbl uuid f = 
		try
			f ()
		with Db_exn.Read_missing_uuid(tbl', "", uuid') when tbl' = tbl && uuid' = uuid -> ()

	let expect_missing_field name f = 
		try
			f ()
		with Db_exn.DBCache_NotFound("missing field", name', "") when name' = name -> ()

	let test_invalid_where_record fn_name fn = 
		Printf.printf "%s <invalid table> ...\n" fn_name;
		expect_missing_tbl "Vm"
			(fun () ->
				let (_: string list) = fn { Db_cache_types.table = "Vm"; return = ""; where_field = ""; where_value = "" } in
				failwith (Printf.sprintf "%s <invalid table>" fn_name)
			);
		Printf.printf "%s <valid table> <invalid return> <valid field> <valid value>\n" fn_name;
		expect_missing_field "wibble"
			(fun () ->
				let (_: string list) = fn { Db_cache_types.table = "VM"; return = "wibble"; where_field = Escaping.escape_id [ "name"; "label" ]; where_value = name } in
				failwith (Printf.sprintf "%s <valid table> <invalid return> <valid field> <valid value>" fn_name)
			);
		Printf.printf "%s <valid table> <valid return> <invalid field> <valid value>\n" fn_name;
		expect_missing_field "wibble"
			(fun () ->
				let (_: string list) = fn { Db_cache_types.table = "VM"; return = Escaping.escape_id [ "name"; "label" ]; where_field = "wibble"; where_value = "" } in
				failwith (Printf.sprintf "%s <valid table> <valid return> <invalid field> <valid value>" fn_name)
			)


	open Pervasiveext
	open Db_cache_types

        let create_db () = Db_ref.Remote

	let main in_process = 	
		(* reference which we create *)
		let valid_ref = "ref1" in
		let valid_uuid = "uuid1" in
		let invalid_ref = "foo" in
		let invalid_uuid = "bar" in
		
		let t = Db_ref.Remote in

	let vbd_ref = "waz" in
		let vbd_uuid = "whatever" in

		(* Before we begin, clear out any old state: *)
		expect_missing_row "VM" valid_ref
			(fun () ->
				Client.delete_row t "VM" valid_ref;
		);

		expect_missing_row "VBD" vbd_ref
		(fun () ->
			Client.delete_row t "VBD" vbd_ref;
		);

		Printf.printf "Deleted stale state from previous test\n";
		
		Printf.printf "get_table_from_ref <invalid ref>\n";
		begin
			match Client.get_table_from_ref t invalid_ref with
				| None -> Printf.printf "Reference '%s' has no associated table\n" invalid_ref
				| Some t -> failwith (Printf.sprintf "Reference '%s' exists in table '%s'" invalid_ref t)
		end;
		Printf.printf "is_valid_ref <invalid_ref>\n";
		if Client.is_valid_ref t invalid_ref then failwith "is_valid_ref <invalid_ref> = true";
		
		Printf.printf "read_refs <valid tbl>\n";
		let existing_refs = Client.read_refs t "VM" in
		Printf.printf "VM refs: [ %s ]\n" (String.concat "; " existing_refs);
		Printf.printf "read_refs <invalid tbl>\n";
		expect_missing_tbl "Vm"
			(fun () ->
				let (_: string list) = Client.read_refs t "Vm" in
				()
			);
		Printf.printf "delete_row <invalid ref>\n";
		expect_missing_row "VM" invalid_ref
			(fun () ->
				Client.delete_row t "VM" invalid_ref;
				failwith "delete_row of a non-existent row silently succeeded"
			);
		Printf.printf "create_row <unique ref> <unique uuid> <missing required field>\n";
		expect_missing_field "name__label"
			(fun () ->
				let broken_vm = List.filter (fun (k, _) -> k <> "name__label") (make_vm valid_ref valid_uuid) in
				Client.create_row t "VM" broken_vm valid_ref;
				failwith "create_row <unique ref> <unique uuid> <missing required field>"
			);
		Printf.printf "create_row <unique ref> <unique uuid>\n";
		Client.create_row t "VM" (make_vm valid_ref valid_uuid) valid_ref;
		Printf.printf "is_valid_ref <valid ref>\n";
		if not (Client.is_valid_ref t valid_ref)
		then failwith "is_valid_ref <valid_ref> = false, after create_row";
		Printf.printf "get_table_from_ref <valid ref>\n";
		begin match Client.get_table_from_ref t valid_ref with
			| Some "VM" -> ()
			| Some t -> failwith "get_table_from_ref <valid ref> : invalid table"
			| None -> failwith "get_table_from_ref <valid ref> : None"
		end;
		Printf.printf "read_refs includes <valid ref>\n";
		if not (List.mem valid_ref (Client.read_refs t "VM"))
		then failwith "read_refs did not include <valid ref>";
		
		Printf.printf "create_row <duplicate ref> <unique uuid>\n";
		expect_uniqueness_violation "VM" "_ref" valid_ref
			(fun () ->
				Client.create_row t "VM" (make_vm valid_ref (valid_uuid ^ "unique")) valid_ref;
				failwith "create_row <duplicate ref> <unique uuid>"
			);
		Printf.printf "create_row <unique ref> <duplicate uuid>\n";
		expect_uniqueness_violation "VM" "uuid" valid_uuid
			(fun () ->
				Client.create_row t "VM" (make_vm (valid_ref ^ "unique") valid_uuid) (valid_ref ^ "unique");
				failwith "create_row <unique ref> <duplicate uuid>"
			);
		Printf.printf "db_get_by_uuid <valid uuid>\n";
		let r = Client.db_get_by_uuid t "VM" valid_uuid in
		if r <> valid_ref
		then failwith (Printf.sprintf "db_get_by_uuid <valid uuid>: got %s; expected %s" r valid_ref);
		Printf.printf "db_get_by_uuid <invalid uuid>\n";
		expect_missing_uuid "VM" invalid_uuid
			(fun () ->
				let (_: string) = Client.db_get_by_uuid t "VM" invalid_uuid in
				failwith "db_get_by_uuid <invalid uuid>"
			);
		Printf.printf "get_by_name_label <invalid name label>\n";
		if Client.db_get_by_name_label t "VM" invalid_name <> []
		then failwith "db_get_by_name_label <invalid name label>";
		
		Printf.printf "get_by_name_label <valid name label>\n";
		if Client.db_get_by_name_label t "VM" name <> [ valid_ref ]
		then failwith "db_get_by_name_label <valid name label>";
		
		Printf.printf "read_field <valid field> <valid objref>\n";
		if Client.read_field t "VM" "name__label" valid_ref <> name
		then failwith "read_field <valid field> <valid objref> : invalid name";

		Printf.printf "read_field <valid defaulted field> <valid objref>\n";
		if Client.read_field t "VM" "protection_policy" valid_ref <> "OpaqueRef:NULL"
		then failwith "read_field <valid defaulted field> <valid objref> : invalid protection_policy";

		Printf.printf "read_field <valid field> <invalid objref>\n";
		expect_missing_row "VM" invalid_ref
			(fun () ->
				let (_: string) = Client.read_field t "VM" "name__label" invalid_ref in
				failwith "read_field <valid field> <invalid objref>"
			);
		Printf.printf "read_field <invalid field> <valid objref>\n";
		expect_missing_field "name_label"
			(fun () ->
				let (_: string) = Client.read_field t "VM" "name_label" valid_ref in
				failwith "read_field <invalid field> <valid objref>"
			);
		Printf.printf "read_field <invalid field> <invalid objref>\n";
		expect_missing_row "VM" invalid_ref
			(fun () ->
				let (_: string) = Client.read_field t "VM" "name_label" invalid_ref in
				failwith "read_field <invalid field> <invalid objref>"
			);
		Printf.printf "read_field_where <valid table> <valid return> <valid field> <valid value>\n";
		let where_name_label = 
			{ Db_cache_types.table = "VM"; return = Escaping.escape_id(["name"; "label"]); where_field="uuid"; where_value = valid_uuid } in
		let xs = Client.read_field_where t where_name_label in
		if not (List.mem name xs)
		then failwith "read_field_where <valid table> <valid return> <valid field> <valid value>";
		test_invalid_where_record "read_field_where" (Client.read_field_where t);
		
		let xs = Client.read_set_ref t where_name_label in
		if not (List.mem name xs)
		then failwith "read_set_ref <valid table> <valid return> <valid field> <valid value>";
		test_invalid_where_record "read_set_ref" (Client.read_set_ref t);
		
		Printf.printf "write_field <invalid table>\n";
		expect_missing_tbl "Vm"
			(fun () ->
				let (_: unit) = Client.write_field t "Vm" "" "" "" in
				failwith "write_field <invalid table>"
			);
		Printf.printf "write_field <valid table> <invalid ref>\n";
		expect_missing_row "VM" invalid_ref
			(fun () ->
				let (_: unit) = Client.write_field t "VM" invalid_ref "" "" in
				failwith "write_field <valid table> <invalid ref>"
			);
		Printf.printf "write_field <valid table> <valid ref> <invalid field>\n";
		expect_missing_field "wibble"
			(fun () ->
				let (_: unit) = Client.write_field t "VM" valid_ref "wibble" "" in
				failwith "write_field <valid table> <valid ref> <invalid field>"
			);
		Printf.printf "write_field <valid table> <valid ref> <valid field>\n";
		let (_: unit) = Client.write_field t "VM" valid_ref (Escaping.escape_id ["name"; "description"]) "description" in
		Printf.printf "write_field <valid table> <valid ref> <valid field> - invalidating ref_index\n";
		let (_: unit) = Client.write_field t "VM" valid_ref (Escaping.escape_id ["name"; "label"]) "newlabel" in

		Printf.printf "read_record <invalid table> <invalid ref>\n";
		expect_missing_tbl "Vm"
			(fun () ->
				let _ = Client.read_record t "Vm" invalid_ref in
				failwith "read_record <invalid table> <invalid ref>"
			);
		Printf.printf "read_record <valid table> <valid ref>\n";
		expect_missing_row "VM" invalid_ref
			(fun () ->
				let _ = Client.read_record t "VM" invalid_ref in
				failwith "read_record <valid table> <invalid ref>"
			);
		Printf.printf "read_record <valid table> <valid ref>\n";
		let fv_list, fvs_list = Client.read_record t "VM" valid_ref in
		if not(List.mem_assoc (Escaping.escape_id [ "name"; "label" ]) fv_list)
		then failwith "read_record <valid table> <valid ref> 1";
		if List.assoc "VBDs" fvs_list <> []
		then failwith "read_record <valid table> <valid ref> 2";
		Printf.printf "read_record <valid table> <valid ref> foreign key\n";
		Client.create_row t "VBD" (make_vbd valid_ref vbd_ref vbd_uuid) vbd_ref;
		let fv_list, fvs_list = Client.read_record t "VM" valid_ref in
		if List.assoc "VBDs" fvs_list <> [ vbd_ref ] then begin
			Printf.printf "fv_list = [ %s ] fvs_list = [ %s ]\n%!" (String.concat "; " (List.map (fun (k, v) -> k ^":" ^ v) fv_list))  (String.concat "; " (List.map (fun (k, v) -> k ^ ":" ^ (String.concat ", " v)) fvs_list));
			failwith "read_record <valid table> <valid ref> 3"
		end;
		Printf.printf "read_record <valid table> <valid ref> deleted foreign key\n";
		Client.delete_row t "VBD" vbd_ref;
		let fv_list, fvs_list = Client.read_record t "VM" valid_ref in
		if List.assoc "VBDs" fvs_list <> []
		then failwith "read_record <valid table> <valid ref> 4";
		Printf.printf "read_record <valid table> <valid ref> overwritten foreign key\n";
		Client.create_row t "VBD" (make_vbd valid_ref vbd_ref vbd_uuid) vbd_ref;
		let fv_list, fvs_list = Client.read_record t "VM" valid_ref in
		if List.assoc "VBDs" fvs_list = []
		then failwith "read_record <valid table> <valid ref> 5";
		Client.write_field t "VBD" vbd_ref (Escaping.escape_id [ "VM" ]) "overwritten";
		let fv_list, fvs_list = Client.read_record t "VM" valid_ref in
		if List.assoc "VBDs" fvs_list <> []
		then failwith "read_record <valid table> <valid ref> 6";	
		
		expect_missing_tbl "Vm"
			(fun () ->
				let _ = Client.read_records_where t "Vm" Db_filter_types.True in
				()
			);
		let xs = Client.read_records_where t "VM" Db_filter_types.True in
		if List.length xs <> 1
		then failwith "read_records_where <valid table> 2";
		let xs = Client.read_records_where t "VM" Db_filter_types.False in
		if xs <> []
		then failwith "read_records_where <valid table> 3";
		
		expect_missing_tbl "Vm"
			(fun () ->
				let _ = Client.find_refs_with_filter t "Vm" Db_filter_types.True in
				failwith "find_refs_with_filter <invalid table>";
			);
		let xs = Client.find_refs_with_filter t "VM" Db_filter_types.True in
		if List.length xs <> 1
		then failwith "find_refs_with_filter <valid table> 1";
		let xs = Client.find_refs_with_filter t "VM" Db_filter_types.False in
		if xs <> []
		then failwith "find_refs_with_filter <valid table> 2";
		
		expect_missing_tbl "Vm"
			(fun () ->
				Client.process_structured_field t ("","") "Vm" "wibble" invalid_ref Db_cache_types.AddSet;
				failwith "process_structure_field <invalid table> <invalid fld> <invalid ref>"
			);
		expect_missing_field "wibble"
			(fun () ->
				Client.process_structured_field t ("","") "VM" "wibble" valid_ref Db_cache_types.AddSet;
				failwith "process_structure_field <valid table> <invalid fld> <valid ref>"
			);
		expect_missing_row "VM" invalid_ref
			(fun () ->
				Client.process_structured_field t ("","") "VM" (Escaping.escape_id ["name"; "label"]) invalid_ref Db_cache_types.AddSet;
				failwith "process_structure_field <valid table> <valid fld> <invalid ref>"
			);
		Client.process_structured_field t ("foo", "") "VM" "tags" valid_ref Db_cache_types.AddSet;
		if Client.read_field t "VM" "tags" valid_ref <> "('foo')"
		then failwith "process_structure_field expected ('foo')";
		Client.process_structured_field t ("foo", "") "VM" "tags" valid_ref Db_cache_types.AddSet;
		if Client.read_field t "VM" "tags" valid_ref <> "('foo')"
		then failwith "process_structure_field expected ('foo') 2";
		Client.process_structured_field t ("foo", "bar") "VM" "other_config" valid_ref Db_cache_types.AddMap;
		
		if Client.read_field t "VM" "other_config" valid_ref <> "(('foo' 'bar'))"
		then failwith "process_structure_field expected (('foo' 'bar')) 3";
		
		begin
			try
				Client.process_structured_field t ("foo", "bar") "VM" "other_config" valid_ref Db_cache_types.AddMap;
			with Db_exn.Duplicate_key("VM", "other_config", r', "foo") when r' = valid_ref -> ()
		end;
		if Client.read_field t "VM" "other_config" valid_ref <> "(('foo' 'bar'))"
		then failwith "process_structure_field expected (('foo' 'bar')) 4"
		
		(* Check that non-persistent fields are filled with an empty value *)

		(* Event tests *)

end

