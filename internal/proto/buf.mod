# vim: set ft=yaml
version: "v1" # this is the buf version
deps:
- "buf.build/beta/protoc-gen-validate"
- "buf.build/authzed/api"
lint:
  except:
    - "ENUM_VALUE_PREFIX"
    - "ENUM_ZERO_VALUE_SUFFIX"
