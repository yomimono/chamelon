type file = {name : string;
             data : Cstruct.t;
            }

let name n id = Tag.({
    valid = true;
    type3 = (Tag.LFS_TYPE_NAME, 0x01);
    id;
    length = String.length n;
  }, Cstruct.of_string n)

let create_inline id contents = Tag.({
    valid = true;
    type3 = (Tag.LFS_TYPE_STRUCT, 0x01);
    id;
    length = (Cstruct.length contents);
  })

let write n id contents =
  [name n id; (create_inline id contents), contents; ]
