
import os
import re
import cv2
from pathlib import Path

def natural_sort_key(s: str):
    """
    Génère une clé de tri qui sépare les parties numériques et non numériques
    pour un tri « naturel » des noms de fichiers.
    """
    return [int(text) if text.isdigit() else text.lower() 
            for text in re.split(r'(\d+)', s)]

def process_images(input_folder: str):
    input_path = Path(input_folder)
    if not input_path.is_dir():
        print(f"Le dossier {input_folder} n'existe pas.")
        return

    # Création du dossier de sortie
    output_path = input_path.parent / f"{input_path.name}_numbered"
    output_path.mkdir(exist_ok=True)
    print(f"Dossier de sortie : {output_path}")

    # Paramètres du texte
    font              = cv2.FONT_HERSHEY_SIMPLEX
    font_scale        = 1       # Échelle du texte (agrandit le texte)
    thickness         = 2       # Épaisseur des traits
    color             = (0, 0, 255)  # Rouge en BGR
    line_type         = cv2.LINE_AA

    # Extensions d'images autorisées
    extensions = {'.jpg', '.jpeg', '.png', '.gif', '.bmp'}
    image_files = [f for f in input_path.iterdir() if f.suffix.lower() in extensions]
    image_files.sort(key=lambda f: natural_sort_key(f.name))

    for img_path in image_files:
        # Lecture de l'image
        img = cv2.imread(str(img_path))
        if img is None:
            print(f"Impossible de lire {img_path}, skip.")
            continue

        # Redimensionnement
        img = cv2.resize(img, (514, 376), interpolation=cv2.INTER_AREA)

        # Texte à dessiner (le nom de fichier sans path)
        text = img_path.name

        # Position du texte (10 px à droite, baseline à 50 px environ)
        position = (10, 50)

        # Dessine le texte
        cv2.putText(img, text, position, font, font_scale, color, thickness, line_type)

        # Enregistrement
        output_file = output_path / img_path.name
        cv2.imwrite(str(output_file), img)
        print(f"Enregistré : {output_file}")

if __name__ == "__main__":
    dossier = "imgs"  # <-- modifiez ici le chemin de votre dossier d'images
    process_images(dossier)
