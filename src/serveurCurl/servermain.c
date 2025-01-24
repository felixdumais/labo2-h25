/* TP2 Hiver 2025 
 * Code source fourni
 * Marc-Andre Gardner
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

// Pour recuperer les descriptions d'erreur
#include <errno.h>

// Multiprocessing
#include <sys/types.h>
#include <sys/wait.h>
#include <sys/time.h>
#include <unistd.h>

// Sockets UNIX
#include <sys/socket.h>
#include <sys/un.h>
#include <fcntl.h>

// Signaux UNIX
#include <signal.h>


// Structures et fonctions de communication
#include "communications.h"
// Fonction de téléchargement utilisant cURL
#include "telechargeur.h"
// Structures et fonctions stockant et traitant les requêtes en cours
#include "requete.h"
// Fonctions de la boucle principale
#include "actions.h"

#include <stddef.h>     // For offsetof

// Chaînes de caractères représentant chaque statut (utile pour l'affichage)
const char* statusDesc[] = {"Inoccupe", "Connexion client etablie", "En cours de telechargement", "Pret a envoyer"};

// Nombre maximal de connexions simultanés
#define MAX_CONNEXIONS 5
// Contient les requetes en cours de traitement
struct requete reqList[MAX_CONNEXIONS];


void gererSignal(int signo) {
    // Fonction affichant des statistiques sur les tâches en cours
    // lorsque SIGUSR2 (et _seulement_ SIGUSR2) est reçu
    // TODO

    if (signo == SIGUSR2) {
        printf("\nStatistiques des tâches en cours:\n");
        for (int i = 0; i < MAX_CONNEXIONS; i++) {
            if (reqList[i].status != REQ_STATUS_INACTIVE) {
                printf("Requête %d: Statut = %s, PID du processus enfant = %d\n",
                       i, statusDesc[reqList[i].status], reqList[i].pid);
            }
        }
        printf("\n");
    }

}



int main(int argc, char* argv[]){
    // Chemin du socket UNIX
    // Linux ne supporte pas un chemin de plus de 108 octets (voir man 7 unix)

    setbuf(stdout, NULL);

    char path[108] = "/tmp/setrunixsocket";
    if(argc > 1)        // On peut également le passer en paramètre
        strncpy(path, argv[1], sizeof(path));
    unlink(path);       // Au cas ou le fichier liant le socket UNIX existerait deja

    // On initialise la liste des requêtes
    memset(&reqList, 0, sizeof(reqList));

    // TODO
    // Implémentez ici le code permettant d'attacher la fonction "gereSignal" au signal SIGUSR2

    struct sigaction sa;
    sa.sa_handler = gererSignal; 
    sigemptyset(&sa.sa_mask);   
    sa.sa_flags = 0;           

    if (sigaction(SIGUSR2, &sa, NULL) == -1) {
        perror("Erreur lors de l'attachement du signal SIGUSR2");
        exit(1);
    }
    printf("Signal SIGUSR2 attache\n");


    // TODO
    // Création et initialisation du socket (il y a 5 étapes)
    // 1) Créez une struct de type sockaddr_un et initialisez-la à 0.
    //      Puis, désignez le socket comme étant de type AF_UNIX
    //      Finalement, copiez le chemin vers le socket UNIX dans le bon attribut de la structure
    //      Voyez man unix(7) pour plus de détails sur cette structure
    struct sockaddr_un un;
    memset(&un, 0, sizeof(un));
    un.sun_family = AF_UNIX;
    strcpy(un.sun_path, path);

    // TODO
    // 2) Créez le socket en utilisant la fonction socket() et affectez-le à une variable nommée sock
    //      Vérifiez si sa création a été effectuée avec succès, sinon quittez le processus en affichant l'erreur
    int sock;
    if ((sock = socket(AF_UNIX, SOCK_STREAM, 0)) < 0) 
    {
        perror("socket failed!");
        exit(1);
    }
    printf("Creation du socket reussie (fd %d)\n", sock);

    // TODO
    // 3) Utilisez fcntl() pour mettre le socket en mode non-bloquant
    //      Vérifiez si l'opération a été effectuée avec succès, sinon quittez le processus en affichant l'erreur
    //      Voyez man fcntl pour plus de détails sur le champ à modifier


    // FDM: fcntl manipule les fd (e.g. sock). Cette fonction permet le controle le comportement des fd.
    // int fcntl(int fd, int op, ... /* arg */ );
    // 1. On vient chercher le statut courant de sock
    // 2. Ensuite on le met en mode non bloquant en ajoutant en ajoutant le bit O_NONBLOCK à l'int flags

    // When dealing with sockets in Linux, non-blocking mode is a feature where socket operations do 
    //  not force the program to wait (block) if the operation cannot be completed immediately. 
    //  Instead, they return control to the program right away.
    // After this, any operations (e.g., read, write, accept, connect, etc.) on the sock will follow non-blocking behavior.
    int flags = fcntl(sock, F_GETFL, 0); 
    if (flags == -1) {
        perror("Erreur lors de la récupération des flags");
        close(sock);
        exit(1);
    }

    if (fcntl(sock, F_SETFL, flags | O_NONBLOCK) == -1) {
        perror("Erreur lors de la mise en mode non-bloquant");
        close(sock);
        exit(1);
    }
    printf("Mode non bloquant actif\n");

    // TODO
    // 4) Faites un bind sur le socket
    //      Vérifiez si l'opération a été effectuée avec succès, sinon quittez le processus en affichant l'erreur
    //      Voyez man bind(2) pour plus de détails sur cette opération
    int size = offsetof(struct sockaddr_un, sun_path) + strlen(un.sun_path);

    if (bind(sock, (struct sockaddr*)&un, size) < 0)
    {
        perror("bind failed!");
        exit(1);
    }
    printf("Bind socket \n");

    // TODO
    // 5) Mettez le socket en mode écoute (listen), en acceptant un maximum de MAX_CONNEXIONS en attente
    //      Vérifiez si l'opération a été effectuée avec succès, sinon quittez le processus en affichant l'erreur
    //      Voyez man listen pour plus de détails sur cette opération

    if (listen(sock, MAX_CONNEXIONS) == -1) {
        perror("Erreur lors de la mise en écoute (listen)");
        close(sock);
        exit(1);
    }
    printf("Listen actif \n");


    // Initialisation du socket UNIX terminée!

    // Boucle principale du programme
    int tacheRealisee;
    while(1){
        // On vérifie si de nouveaux clients attendent pour se connecter
        tacheRealisee = verifierNouvelleConnexion(reqList, MAX_CONNEXIONS, sock);

        // On teste si un client vient de nous envoyer une requête
        // Si oui, on la traite
        tacheRealisee += traiterConnexions(reqList, MAX_CONNEXIONS);

        // On teste si un de nos processus enfants a terminé son téléchargement
        // Dans ce cas, on traite le résultat
        tacheRealisee += traiterTelechargements(reqList, MAX_CONNEXIONS);

        // Si on a des données à envoyer au client, on le fait
        tacheRealisee += envoyerReponses(reqList, MAX_CONNEXIONS);

        // Si on n'a pas attendu dans un select ou effectué une tâche, on ajoute
        // un petit delai ici pour éviter d'utiliser 100% du CPU inutilement
        if(tacheRealisee == 0)
            usleep(SLEEP_TIME);
    }

    return 0;
}
