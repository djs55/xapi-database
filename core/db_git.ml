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
        Store.update store ["foo"; "bar"] "stuff")
end
