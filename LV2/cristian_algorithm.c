#include <mpi.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <time.h>

double get_local_time() {
    struct timespec ts;
    clock_gettime(CLOCK_REALTIME, &ts);
    return ts.tv_sec + ts.tv_nsec / 1e9;
}

int main(int argc, char** argv) {
    MPI_Init(&argc, &argv);

    int rank;
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);

    if (rank == 0) {
        // Server
        while (1) {
            double dummy;
            MPI_Status status;
            MPI_Recv(&dummy, 1, MPI_DOUBLE, 1, 0, MPI_COMM_WORLD, &status);
            double server_time = get_local_time();
            MPI_Send(&server_time, 1, MPI_DOUBLE, 1, 0, MPI_COMM_WORLD);
            if (dummy < 0) break;
        }
    } else if (rank == 1) {
        for (int i = 0; i < 3; i++) {
            // Time before request
            double t1 = get_local_time();
            MPI_Send(&t1, 1, MPI_DOUBLE, 0, 0, MPI_COMM_WORLD);
            double server_time;
            MPI_Recv(&server_time, 1, MPI_DOUBLE, 0, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
            // Time after reply
            double t2 = get_local_time();

            double RTT = (t2 - t1);
            double offset = server_time + RTT / 2 - t2;
            printf("Round %d: RTT=%.6fs Offset=%.6fs\n", i+1, RTT, offset);
            sleep(1);
        }
        double end = -1;
        MPI_Send(&end, 1, MPI_DOUBLE, 0, 0, MPI_COMM_WORLD);
    }

    MPI_Finalize();
    return 0;
}
