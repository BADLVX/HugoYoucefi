@echo off
:menu
cls
echo 1. Afficher l'heure du moment
echo 2. Afficher la date du moment
echo 3. Quitter

set /p choix="Choisissez une option (1, 2, ou 3): "

if "%choix%"=="1" (
    echo L'heure actuelle est :
    time /t
    pause
    goto menu
) else if "%choix%"=="2" (
    echo La date actuelle est :
    date /t
    pause
    goto menu
) else if "%choix%"=="3" (
    echo Au revoir !
    exit /b 0
) else (
    echo Choix non valide. Veuillez choisir une option valide.
    pause
    goto menu
)
