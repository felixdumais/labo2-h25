/* TP2 Hiver 2025 
 * Code source fourni
 * Marc-Andre Gardner
 */

#include "actions.h"
#include <sys/socket.h>


int verifierNouvelleConnexion(struct requete reqList[], int maxlen, int socket){
    // Dans cette fonction, vous devez d'abord vérifier si le serveur peut traiter
    // une nouvelle connexions (autrement dit, si le nombre de connexions en cours
    // ne dépasse pas MAX_CONNEXIONS). Utilisez nouvelleRequete() pour cela.
    //
    // Si une nouvelle connexion peut être traitée, alors vous devez utiliser accept()
    // pour vérifier si un nouveau client s'est connecté. Si c'est le cas, vous devez modifier
    // la nouvelle entrée de reqList pour y sauvegarder le descripteur de fichier correspondant
    // à cette nouvelle connexion, et changer son statut à REQ_STATUS_LISTEN
    // Voyez man accept(2) pour plus de détails sur cette fonction
    // Note importante : vous devez vous assurer que accept() ne produise pas d'erreur, mais 
    // faites attention, certaines erreurs peuvent parfois être normales dans le contexte de
    // votre programme!
    //
    // Cette fonction doit retourner 0 si elle n'a pas acceptée de nouvelle connexion, ou 1 dans le cas contraire.

    // TODO

    int index_to_place_socket;
    if ((index_to_place_socket = nouvelleRequete(reqList, maxlen)) < 0) {
        return 0; 
    }

    struct sockaddr_un client_addr;
    socklen_t addrLen = sizeof(client_addr);

    int client_socket = accept(socket, (struct sockaddr *)&client_addr, &addrLen);
    if (client_socket < 0) {
        if (errno == EAGAIN ) {
            return 0;
        } else {
            perror("Erreur lors de l'acceptation d'une nouvelle connexion");
            return 0;
        }
    }
    printf("Nouvelle connection (fd %d)\n", client_socket);

    reqList[index_to_place_socket].fdSocket = client_socket; 
    reqList[index_to_place_socket].status = REQ_STATUS_LISTEN; 
    reqList[index_to_place_socket].pid = 0; 
    reqList[index_to_place_socket].fdPipe = -1; 
    reqList[index_to_place_socket].buf = NULL; 
    reqList[index_to_place_socket].len = 0; 

    return 1;
}

int traiterConnexions(struct requete reqList[], int maxlen){
    // Cette fonction est partiellement implémentée pour vous
    // Elle utilise select() pour déterminer si une connexion cliente vient d'envoyer
    // une requête (et s'il faut donc la lire).
    // Si c'est le cas, elle lit la requête et la stocke dans un buffer.
    //
    // Par la suite, VOUS devez implémenter le code créant un nouveau processus et
    // un nouveau pipe de communication, partagé avec ce processus enfant.
    // Finalement, vous devez mettre à jour la structure de données de la requête touchée.
    // Cette fonction doit retourner 0 si elle n'a lu aucune donnée supplémentaire, ou un nombre > 0 si c'est le cas.

    int octetsTraites;

    // On parcourt la liste des connexions en cours
    // On utilise select() pour determiner si des descripteurs de fichier sont disponibles
    fd_set setSockets;
    struct timeval tInfo;
    tInfo.tv_sec = 0;
    tInfo.tv_usec = SLEEP_TIME;
    int maxFileDescriptorPlusOne = 0;
    FD_ZERO(&setSockets);

    for(int i = 0; i < maxlen; ++i){
        if(reqList[i].status == REQ_STATUS_LISTEN){
            FD_SET(reqList[i].fdSocket, &setSockets);
            maxFileDescriptorPlusOne = (maxFileDescriptorPlusOne < reqList[i].fdSocket+1) ? reqList[i].fdSocket+1 : maxFileDescriptorPlusOne;
        }
    }

    if(maxFileDescriptorPlusOne){
        // Au moins un socket est en attente d'une requête
        // select attend comme premier argument le descripteur de fichier ayant la valeur maximale plus 1
        int s = select(maxFileDescriptorPlusOne, &setSockets, NULL, NULL, &tInfo);
        if(s > 0){
            // Au moins un socket est prêt à être lu
            for(int i = 0; i < maxlen; ++i){
                if(reqList[i].status == REQ_STATUS_LISTEN && FD_ISSET(reqList[i].fdSocket, &setSockets)){
                    struct msgReq req;
                    char* buffer = malloc(sizeof(req));

                    // On lit les donnees sur le socket
                    if(VERBOSE)
                        printf("traiterConnexion(): Lecture de la requete sur le socket %i\n", reqList[i].fdSocket);
                    octetsTraites = read(reqList[i].fdSocket, buffer, sizeof(req));
                    if(octetsTraites == -1){
                        perror("Erreur en effectuant un read() sur un socket pret");
                        exit(1);
                    }

                    memcpy(&req, buffer, sizeof(req));
                    buffer = realloc(buffer, sizeof(req) + req.sizePayload);
                    octetsTraites = read(reqList[i].fdSocket, buffer + sizeof(req), req.sizePayload);
                    if(VERBOSE){
                        printf("traiterConnexion(): \t%i octets lus au total\n", req.sizePayload + sizeof(req));
                        printf("traiterConnexion(): \tContenu de la requete : %s\n", buffer + sizeof(req));
                    }

                    // Ici, vous devez tout d'abord initialiser un nouveau pipe à l'aide de la fonction pipe()
                    // Voyez man pipe pour plus d'informations sur son fonctionnement
                    // TODO

                    // Une fois le pipe initialisé, vous devez effectuer un fork, à l'aide de la fonction du même nom
                    // Cela divisera votre processus en deux nouveaux processus, un parent et un enfant.
                    // - Dans le processus enfant, vous devez appeler la fonction executerRequete() en lui donnant
                    //      l'extrémité d'écriture du pipe et le buffer contenant la requête. Lorsque cette fonction
                    //      retourne, vous pouvez assumer que le téléchargement est terminé et quitter le processus.
                    // - Dans le processus parent, vous devez enregistrer le PID (id du processus) de l'enfant ainsi que
                    //      le descripteur de fichier de l'extrémité de lecture du pip dans la structure de la requête.
                    //      Vous devez également passer son statut à REQ_STATUS_INPROGRESS.
                    //
                    // Pour plus d'informations sur la fonction fork() et sur la manière de détecter si vous êtes dans
                    // le parent ou dans l'enfant, voyez man fork(2).
                    // TODO

                    int pipefd[2];
                    if (pipe(pipefd) == -1) {
                        perror("Erreur lors de la création du pipe");
                        exit(1);
                    }

                    pid_t pid = fork();
                    if (pid == -1) {
                        perror("Erreur lors du fork");
                        exit(1);
                    }

                    if (pid == 0) {
                        printf("traiterConnexion(): Je suis le PID %d\n", pid);
                        close(pipefd[0]);

                        executerRequete(pipefd[1], buffer);

                        close(pipefd[1]);

                        exit(0); 
                    } else { 
                        printf("traiterConnexion(): Je suis le PID %d\n", pid);
                        close(pipefd[1]);

                        reqList[i].pid = pid;
                        reqList[i].fdPipe = pipefd[0];
                        reqList[i].status = REQ_STATUS_INPROGRESS;

                       if(VERBOSE){
                           printf("traiterConnexion(): Processus enfant créé avec PID : %d\n", pid);
                           printf("traiterConnexion(): Descripteur de fichier du pipe (lecture) : %d\n", reqList[i].fdPipe);
                       }
                    }

                    free(buffer); // Libérer la mémoire allouée pour le buffer

                }
            }
        }
    }

    return maxFileDescriptorPlusOne;
}


int traiterTelechargements(struct requete reqList[], int maxlen){
    // Cette fonction détermine si des processus enfants (s'il y en a) ont écrit quelque chose sur leur pipe.
    // Si c'est le cas, elle le lit et le stocke dans le buffer lié à la requête.
    //
    // Il vous est conseillé de vous inspirer de la fonction traiterConnexions(), puisque la procédure y est
    // très similaire. En détails, vous devez :
    // 1) Faire la liste des descripteurs qui correspondent à des pipes ouverts
    // 2) Utiliser select() pour déterminer si un de ceux-ci peut être lu
    // 3) Si c'est le cas, vous devez lire son contenu. Rappelez-vous (voir les commentaires dans telecharger.h) que
    //      le processus enfant écrit d'abord la taille du contenu téléchargé, puis le contenu téléchargé lui-même.
    //      Cela vous permet de savoir combien d'octets vous devez récupérer au total. Attention : plusieurs lectures
    //      successives peuvent être nécessaires pour récupérer tout le contenu du pipe!
    // 4) Une fois que vous avez récupéré son contenu, modifiez le champ len de la structure de la requête pour
    //      refléter la taille du fichier, ainsi que buffer pour y écrire un pointeur vers les données. Modifiez
    //      également le statut à REQ_STATUS_READYTOSEND.
    // 5) Finalement, terminer les opérations avec le processus enfant le rejoignant en attendant sa terminaison
    //      (vous aurez besoin de la fonction waitpid()), puis fermer le descripteur correspondant l'extrémité
    //      du pipe possédée par le parent.
    //
    // S'il n'y a aucun processus enfant lancé, ou qu'aucun processus n'a écrit de données, cette fonction
    // peut retourner sans aucun traitement.
    // Cette fonction doit retourner 0 si elle n'a lu aucune donnée supplémentaire, ou un nombre > 0 si c'est le cas.

    // TODO

    int octetsTraites = 0;

    fd_set setPipes;
    struct timeval tInfo;
    tInfo.tv_sec = 0;
    tInfo.tv_usec = SLEEP_TIME;
    int maxFileDescriptorPlusOne = 0;
    FD_ZERO(&setPipes);

    for(int i = 0; i < maxlen; ++i){
        if(reqList[i].status == REQ_STATUS_INPROGRESS){
            FD_SET(reqList[i].fdPipe, &setPipes);
            maxFileDescriptorPlusOne = (maxFileDescriptorPlusOne < reqList[i].fdPipe+1) ? reqList[i].fdPipe+1 : maxFileDescriptorPlusOne;
        }
    }

    if(maxFileDescriptorPlusOne){
        int s = select(maxFileDescriptorPlusOne, &setPipes, NULL, NULL, &tInfo);
        if(s > 0){
            for(int i = 0; i < maxlen; ++i){
                if(reqList[i].status == REQ_STATUS_INPROGRESS && FD_ISSET(reqList[i].fdPipe, &setPipes)){
                    int tailleContenu;
                    int lectureTotale = 0;
                    int lectureCourante;

                    if (read(reqList[i].fdPipe, &tailleContenu, sizeof(int)) != sizeof(int)) {
                        perror("Erreur lors de la lecture de la taille du contenu depuis le pipe");
                        close(reqList[i].fdPipe);
                        // reqList[i].status = REQ_STATUS_ERROR;
                        continue; // Passer à la requête suivante
                    }


                    reqList[i].buf = malloc(tailleContenu);
                    if (reqList[i].buf == NULL) {
                        perror("Erreur d'allocation de mémoire");
                        exit(1);
                    }

                    // Lire le contenu
                    while (lectureTotale < tailleContenu) {
                        lectureCourante = read(reqList[i].fdPipe, reqList[i].buf + lectureTotale, tailleContenu - lectureTotale);
                        if (lectureCourante == -1) {
                            perror("Erreur lors de la lecture du contenu depuis le pipe");
                            free(reqList[i].buf);
                            close(reqList[i].fdPipe);
                            // reqList[i].status = REQ_STATUS_ERROR;
                            continue;
                        }
                        lectureTotale += lectureCourante;
                    }


                    reqList[i].len = tailleContenu;
                    reqList[i].status = REQ_STATUS_READYTOSEND;

                    // Attendre la fin du processus enfant et fermer le pipe
                    int status;
                    waitpid(reqList[i].pid, &status, 0);
                    close(reqList[i].fdPipe);

                    if(VERBOSE){
                        printf("Téléchargement terminé pour le PID : %d\n", reqList[i].pid);
                        printf("Taille du fichier téléchargé : %d octets\n", reqList[i].len);
                    }
                    octetsTraites++;
                }
            }
        }
    }

    return octetsTraites;
}
