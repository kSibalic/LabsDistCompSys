#ifndef RPC_H
#define RPC_H

#include <cstring>

enum RPC_procedure { PROCEDURE_SET = 2, PROCEDURE_GET = 3 };

struct RPC_message {
  RPC_procedure procedure;
  union {
    struct {
      int x, y;
    } add_args;
    struct {
      char key[64];
      char value[64];
    } set_args;
    struct {
      char key[64];
    } get_args;
  } args;
};
#endif
