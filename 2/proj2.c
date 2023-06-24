/**
 * @file proj2.c
 * @brief Post
 * @author Karolína Virágová <xvirag00@stud.fit.vutbr.cz>
 * @date 15. 4. 2023
 * @version 1.0
 */

#include <pthread.h>
#include <semaphore.h>
#include <stdarg.h>
#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <sys/ipc.h>
#include <sys/shm.h>
#include <sys/wait.h>
#include <time.h>
#include <unistd.h>

/// NZ: počet zákazníků
/// NU: počet úředníků
/// TZ: Maximální čas v milisekundách, po který zákazník po svém vytvoření čeká,
/// než vejde na poštu (eventuálně odchází s nepořízenou). 0<=TZ<=10000
/// TU: Maximální délka přestávky úředníka v milisekundách. 0<=TU<=100
/// F: Maximální čas v milisekundách, po kterém je uzavřena pošta pro nově
/// příchozí. 0<=F<=10000
#define EXPECTED_ARGUMENTS_COUNT 6 /// Expected number of program arguments.
#define MIN 0                      /// minimum pro TZ, TU, F
#define MAX_TZ 10000               /// maximální čas v milisekundách TZ, F
#define MAX_TU 100                 /// maximální délka F

typedef struct arguments {
  int NZ;
  int NU;
  int TZ;
  int TU;
  int F;
} Targuments;

/// sdílená paměť
typedef struct {
  int counter;
  sem_t sem_file;
  FILE *file;

  sem_t wait_queues[3];

  sem_t sem_services;
  int services[3];

  sem_t post;
  bool post_office_open;
} shared_data;

int shmid;
shared_data *shm;

/// zápis do souboru
void print(shared_data *shm, const char *format, ...) {
  sem_wait(&shm->sem_file);
  va_list args;
  va_start(args, format);
  fprintf(shm->file, "%u: ", shm->counter++);
  vfprintf(shm->file, format, args);
  fflush(shm->file);
  va_end(args);
  sem_post(&shm->sem_file);
}

///zjištění úředníkem zda se náchází zákazník ve frontě
int service(shared_data *shm) {
  int random = rand() % 3;
  if ((shm->services[random % 3]) > 0) {
    return random % 3;
  } else if ((shm->services[(random + 1) % 3]) > 0) {
    return (random + 1) % 3;
  } else if ((shm->services[(random + 2) % 3]) > 0) {
    return (random + 2) % 3;
  } else {
    return -1;
  }
}

/// @brief obsluha úředníka na poště, úředník poskytuje služby zákazníkovi
/// @param shm
/// @param id_clerk
void clerk_process(shared_data *shm, int id_clerk, Targuments *arguments) {
  srand(getpid() * time(NULL));
  int cislo_fronty;
  print(shm, "U %d: started\n", id_clerk);

  while (true) {

    sem_wait(&shm->post);
    bool opened = shm->post_office_open;
    sem_post(&shm->post);

    sem_wait(&shm->sem_services); // tady ceka clerk
    cislo_fronty = service(shm);

    if (cislo_fronty != -1) {
      print(shm, "U %d: serving a service of type  %d\n", id_clerk,
            cislo_fronty + 1);
      shm->services[cislo_fronty]--;
      sem_post(&shm->sem_services);
      sem_post(&shm->wait_queues[cislo_fronty]);
      usleep(rand() % (10 + 1));
      print(shm, "U %d: service finished\n", id_clerk);
      continue;
    }

    if (opened) {
      sem_post(&shm->sem_services);
      print(shm, "U %d: taking break\n", id_clerk);

      usleep(rand() % (arguments->TU + 1));

      print(shm, "U %d: break finished\n", id_clerk);
      continue;
    } else {
      sem_post(&shm->sem_services);
      print(shm, "U %d: going home\n", id_clerk);
      exit(0);
    }
  }
}
/// @brief obsluha zákazníka dle požedavků
/// @param shm
/// @param id_customer
void customer_process(shared_data *shm, int id_customer,
                      Targuments *arguments) {
  srand(getpid() * time(NULL));
  print(shm, "Z %d: started\n", id_customer);
  usleep(rand() % (arguments->TZ + 1));

  // zjištění zda je pošta zavřená, pokud je zákazník vypíše going home
  sem_wait(&shm->post);
  if (shm->post_office_open == false) {
    sem_post(&shm->post);
    print(shm, "Z %d: going home\n", id_customer);
    exit(0);
  } else {
    sem_post(&shm->post);
    int random = (rand() % 3);

    sem_wait(&shm->sem_services); // tady je customer
    shm->services[random]++;
    sem_post(&shm->sem_services);

    print(shm, "Z %d: entering office for a service %d\n", id_customer,
          random + 1);
    // cekame na vyrizeni urednikem
    sem_wait(&shm->wait_queues[random]);

    print(shm, "Z %d: called by office worker\n", id_customer);

    usleep(rand() % (10 + 1));

    print(shm, "Z %d: going home\n", id_customer);
    exit(0);
  }
}

/// @brief parsování argumentů a ošetření chybových vstupů
/// @param argc
/// @param argv
/// @param arguments
int pars_arguments(int argc, char *argv[], Targuments *arguments) {

  if (argc != EXPECTED_ARGUMENTS_COUNT) {
    fprintf(stderr, "Nesprávný počet argumentů\n");
    exit(1);
    return EXIT_FAILURE;
  } else {

    arguments->NZ = atoi(argv[1]);
    arguments->NU = atoi(argv[2]);
    arguments->TZ = atoi(argv[3]) * 1000;
    arguments->TU = atoi(argv[4]) * 1000;
    arguments->F = atoi(argv[5]) * 1000;

    if (arguments->NU == 0 ||
        !(0 <= arguments->TZ && arguments->TZ <= 10000 * 1000) ||
        !(0 <= arguments->TU && arguments->TU <= 100 * 1000) ||
        !(0 <= arguments->F && arguments->F <= 10000 * 1000)) {
      fprintf(stderr, "Zadaný argument je mim rozsah.\n");
      exit(1);
    }
  }
  return EXIT_SUCCESS;
}

int main(int argc, char *argv[]) {
  srand(getpid() * time(NULL));
  Targuments arguments;

  pars_arguments(argc, argv, &arguments);

  // vytvoření sdílené paměti
  if ((shmid = shmget(IPC_PRIVATE, sizeof(shared_data),
                      IPC_CREAT | IPC_EXCL | 0666)) == 0 ||
      (shm = shmat(shmid, NULL, 0)) == NULL) {
    fprintf(stderr, "Nastala chyba při alokaci sdílené paměti.");
    exit(1);
  }

  if (sem_init(&shm->sem_file, 1, 1) == -1 ||
      sem_init(&shm->wait_queues[0], 1, 0) == -1 ||
      sem_init(&shm->wait_queues[1], 1, 0) == -1 ||
      sem_init(&shm->wait_queues[2], 1, 0) == -1 ||
      sem_init(&shm->sem_services, 1, 1) == -1 ||
      sem_init(&shm->post, 1, 1) == -1) {
    fprintf(stderr, "Nastala chyba při vytváření semaforů");
    exit(1);
  }
  // inicializace sdílených dat
  shm->counter = 1;
  shm->post_office_open = true;
  shm->services[0] = 0;
  shm->services[1] = 0;
  shm->services[2] = 0;

  /// ovtevření souboru pro zápis
  if ((shm->file = fopen("proj2.out", "w")) == NULL) {
    fprintf(stderr, "Chyba při otevření souboru\n");
    return 1;
  }

  /// úředníci
  for (int i = 1; i < arguments.NU + 1; i++) {
    int pid = fork();
    if (pid == 0) {
      clerk_process(shm, i, &arguments);
      exit(0);
    } else if (pid < 0) {
      fprintf(stderr, "Chyba při tvoření procesu úředníka\n");
      exit(0);
    }
  }

  /// zákazníci
  for (int i = 1; i < arguments.NZ + 1; i++) {
    int pid = fork();
    if (pid == 0) {
      customer_process(shm, i, &arguments);
      exit(0);
    } else if (pid < 0) {
      fprintf(stderr, "Chyba při tvoření procesu zákazníka\n");
      exit(0);
    }
  }

  usleep(rand() % ((arguments.F + 1) / 2) + (arguments.F / 2));
  sem_wait(&shm->post);
  shm->post_office_open = false;
  sem_post(&shm->post);
  print(shm, "closing\n");

  // čekání na procesy potomků
  for (int i = 0; i < arguments.NU + arguments.NZ; i++) {
    int status;
    wait(&status);
    if (!WIFEXITED(status))
      fprintf(stderr, "Proces nam neco zabilo");
    if (WEXITSTATUS(status) != 0)
      fprintf(stderr, "Proces skoncil s chybou");
  }

  // uvolnění semaforů
  if (sem_destroy(&shm->sem_file) == -1 ||
      sem_destroy(&shm->wait_queues[0]) == -1 ||
      sem_destroy(&shm->wait_queues[1]) == -1 ||
      sem_destroy(&shm->wait_queues[2]) == -1 ||
      sem_destroy(&shm->sem_services) == -1 || 
      sem_destroy(&shm->post) == -1) {
    fprintf(stderr, "Nepodarilo sa smazat semafory.\n");
    exit(1);
  }

  // uvolnění sdílené paměti
  if ( shmctl(shmid, IPC_RMID, NULL) == -1 || shmdt(shm) == -1) {
    fprintf(stderr, "Nepodarilo sa uvolnit sdílenou paměť.\n");
    exit(1);

    fclose(shm->file);
    exit(0);
  }
}
