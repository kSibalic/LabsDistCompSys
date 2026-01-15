#include <cstdint>
#include <cstdlib>

enum RPC_procedure : uint32_t { PROCEDURE_SET = 2, PROCEDURE_GET = 3 };

const size_t KEY_SIZE = 64;
const size_t VALUE_SIZE = 64;
