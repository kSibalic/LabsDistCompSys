#include <mpi.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>

int L = 0;

int on_send() {
    L = L + 1;
    return L;
}

int on_receive(int L_msg) {
    if (L_msg > L)
        L = L_msg;
    L = L + 1;
    return L;
}

int main(int argc, char** argv) {
    MPI_Init(&argc, &argv);

    int rank;
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);

    if (rank == 0) {
        // Simulate offset (slower)
        sleep(2);
        for (int seq = 1; seq <= 3; seq++) {
            int L_out = on_send();
            int payload[2] = { seq, L_out };
            MPI_Send(payload, 2, MPI_INT, 1, 0, MPI_COMM_WORLD);
            printf("[A] Sent seq=%d L=%d (after delay)\n", seq, L_out);
            fflush(stdout);
            sleep(1);
        }
        int end[2] = { -1, -1 };
        MPI_Send(end, 2, MPI_INT, 1, 0, MPI_COMM_WORLD);
    }  
    else if (rank == 1) {
        // Simulate offset (faster)
        usleep(500000);
        while (1) {
            int payload[2];
            MPI_Status status;
            MPI_Recv(payload, 2, MPI_INT, 0, 0, MPI_COMM_WORLD, &status);
            if (payload[0] == -1) break;
            int seq = payload[0];
            int L_in = payload[1];
            int L_after = on_receive(L_in);
            printf("[B] Received seq=%d L_in=%d L_after=%d (with drift)\n", seq, L_in, L_after);
            fflush(stdout);
        }
    }

    MPI_Finalize();
    return 0;
}
