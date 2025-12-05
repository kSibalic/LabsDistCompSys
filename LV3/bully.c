#include <mpi.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>

#define ELECTION 1
#define OK_MSG 2
#define COORDINATOR 3

int main(int argc, char **argv) {
    MPI_Init(&argc, &argv);
    int rank, size;
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &size);
    int disabled = -1; 
    
    if (argc >= 2) disabled = atoi(argv[1]);
    
    int msg_count = 0;
    int leader = size - 1;
    MPI_Barrier(MPI_COMM_WORLD);
    double t0 = 0.0, t1 = 0.0;
    int initiator = 0;
    MPI_Status status;

    if (rank == initiator && disabled == leader) {
        t0 = MPI_Wtime();
        for (int j = rank + 1; j < size; j++) {
            if (j == disabled) continue;
            MPI_Send(NULL, 0, MPI_INT, j, ELECTION, MPI_COMM_WORLD);
            msg_count++;
        }
    }

    double start_loop = MPI_Wtime();
    while (MPI_Wtime() - start_loop < 2.0) {
        int flag = 0;
        MPI_Iprobe(MPI_ANY_SOURCE, MPI_ANY_TAG, MPI_COMM_WORLD, &flag, &status);

        if (flag) {
            if (status.MPI_TAG == ELECTION) {
                MPI_Recv(NULL, 0, MPI_INT, status.MPI_SOURCE, ELECTION, MPI_COMM_WORLD, &status);
                
                if (rank == disabled) continue;

                MPI_Send(NULL, 0, MPI_INT, status.MPI_SOURCE, OK_MSG, MPI_COMM_WORLD);
                msg_count++;

                for (int j = rank + 1; j < size; j++) {
                    if (j == disabled) continue;
                    MPI_Send(NULL, 0, MPI_INT, j, ELECTION, MPI_COMM_WORLD);
                    msg_count++;
                }
            }
            else if (status.MPI_TAG == OK_MSG) {
                MPI_Recv(NULL, 0, MPI_INT, status.MPI_SOURCE, OK_MSG, MPI_COMM_WORLD, &status);
            }
            else if (status.MPI_TAG == COORDINATOR) {
                MPI_Recv(&leader, 1, MPI_INT, status.MPI_SOURCE, COORDINATOR, MPI_COMM_WORLD, &status);
                t1 = MPI_Wtime();
                break;
            }
        }

        if (rank == size - 2 && disabled == size - 1) {
            for (int j = 0; j < size; j++) {
                if (j == disabled || j == rank) continue;
                MPI_Send(&rank, 1, MPI_INT, j, COORDINATOR, MPI_COMM_WORLD);
                msg_count++;
            }
            leader = rank;
            t1 = MPI_Wtime();
            break;
        }
        usleep(1000);
    }

    int total_msgs = 0;
    MPI_Reduce(&msg_count, &total_msgs, 1, MPI_INT, MPI_SUM, 0, MPI_COMM_WORLD);
    MPI_Barrier(MPI_COMM_WORLD);

    if (rank == 0) {
        double elapsed = (t1 > t0) ? t1 - t0 : 0.0;
        printf("Bully | N=%d | disabled=%d | total_msgs=%d | time=%.6f sec\n",
               size, disabled, total_msgs, elapsed);
    }
    MPI_Finalize();
    return 0;
}
