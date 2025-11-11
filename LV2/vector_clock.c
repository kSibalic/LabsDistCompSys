#include <mpi.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>

#define N 2

void print_vector(int* V) {
    printf("[%d,%d]", V[0], V[1]);
}

void update_vector_send(int* V, int rank) {
    V[rank] += 1;
}

void update_vector_receive(int* V, int* V_msg, int rank) {
    for (int i = 0; i < N; i++) {
        if (V_msg[i] > V[i])
            V[i] = V_msg[i];
    }
    V[rank] += 1;
}

int main(int argc, char** argv) {
    MPI_Init(&argc, &argv);

    int rank;
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);

    int V[N] = {0, 0};

    if (rank == 0) {
        for (int seq = 1; seq <= 3; seq++) {
            update_vector_send(V, rank);
            MPI_Send(V, N, MPI_INT, 1, 0, MPI_COMM_WORLD);
            printf("[A] Sent seq=%d ", seq);
            print_vector(V);
            printf("\n");
            sleep(1);
        }
        int end[N] = {-1, -1};
        MPI_Send(end, N, MPI_INT, 1, 0, MPI_COMM_WORLD);
    } else if (rank == 1) {
        while (1) {
            int V_msg[N];
            MPI_Status status;
            MPI_Recv(V_msg, N, MPI_INT, 0, 0, MPI_COMM_WORLD, &status);
            if (V_msg[0] == -1) break;
            update_vector_receive(V, V_msg, rank);
            printf("[B] Received ");
            print_vector(V_msg);
            printf(" -> Updated ");
            print_vector(V);
            printf("\n");
        }
    }

    MPI_Finalize();
    return 0;
}
