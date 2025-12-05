#include <mpi.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>

#define ELECTION 1
#define COORDINATOR 2

int main(int argc, char **argv) {
    MPI_Init(&argc, &argv);
    int rank, size;
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &size);
    int initiator = 0;
    int disabled = -1;

    if (argc >= 2) initiator = atoi(argv[1]);
    if (argc >= 3) disabled = atoi(argv[2]);

    int successor = (rank + 1) % size;
    int leader = -1;
    int msg_count = 0;
    MPI_Status status;
    int *buffer = malloc(sizeof(int) * size);
    for (int i = 0; i < size; i++) buffer[i] = -1;
    MPI_Barrier(MPI_COMM_WORLD);
    double t0 = 0.0, t1 = 0.0;

    if (rank == initiator && rank != disabled) {
        buffer[0] = rank;
        MPI_Send(buffer, size, MPI_INT, successor, ELECTION, MPI_COMM_WORLD);
        msg_count++;
        t0 = MPI_Wtime();
    }

    double start_loop = MPI_Wtime();

    while (MPI_Wtime() - start_loop < 5.0) {
        int flag = 0;
        MPI_Iprobe(MPI_ANY_SOURCE, MPI_ANY_TAG, MPI_COMM_WORLD, &flag, &status);

        if (flag) {
            if (status.MPI_TAG == ELECTION) {
                int *recvbuf = malloc(sizeof(int) * size);
                MPI_Recv(recvbuf, size, MPI_INT, status.MPI_SOURCE, ELECTION, MPI_COMM_WORLD, &status);

                int pos = -1;
                for (int i = 0; i < size; i++)
                    if (recvbuf[i] == -1) { pos = i; break; }
                if (pos != -1) recvbuf[pos] = rank;

                if (recvbuf[0] == initiator && rank == initiator) {
                    int maxid = -1;
                    for (int i = 0; i < size; i++)
                        if (recvbuf[i] > maxid) maxid = recvbuf[i];

                    leader = maxid;
                    MPI_Send(&leader, 1, MPI_INT, successor, COORDINATOR, MPI_COMM_WORLD);
                    msg_count++;
                    t1 = MPI_Wtime();
                    free(recvbuf);
                    break;
                }
                MPI_Send(recvbuf, size, MPI_INT, successor, ELECTION, MPI_COMM_WORLD);
                msg_count++;

                free(recvbuf);
            }
            else if (status.MPI_TAG == COORDINATOR) {
                int new_leader;
                MPI_Recv(&new_leader, 1, MPI_INT, status.MPI_SOURCE, COORDINATOR, MPI_COMM_WORLD, &status);

                if (rank == disabled) continue;

                leader = new_leader;
                MPI_Send(&leader, 1, MPI_INT, successor, COORDINATOR, MPI_COMM_WORLD);
                msg_count++;

                if (rank == initiator) {
                    t1 = MPI_Wtime();
                    break;
                }
            }
        }
        usleep(1000);
    }

    int total_msgs = 0;
    MPI_Reduce(&msg_count, &total_msgs, 1, MPI_INT, MPI_SUM, 0, MPI_COMM_WORLD);
    MPI_Barrier(MPI_COMM_WORLD);

    if (rank == 0) {
        double elapsed = (t1 > t0) ? t1 - t0 : 0.0;
        printf("Ring | N=%d | initiator=%d | disabled=%d | total_msgs=%d | time=%.6f sec\n",
               size, initiator, disabled, total_msgs, elapsed);
    }

    free(buffer);
    MPI_Finalize();
    return 0;
}
