# Simulación de compra de pasajes

Página estática en español con una simulación visual de compra concurrente de pasajes de bus.

## Publicar con GitHub Pages

1. Crea un repositorio público en GitHub.
2. Sube index.html, simulation.js, simulation.css y flores_hermanos.c directamente a la raíz del repositorio. Incluye .nojekyll si es posible.
3. Abre Settings > Pages.
4. En Build and deployment, elige Deploy from a branch.
5. Selecciona la rama main, la carpeta / (root) y pulsa Save.
6. Espera la publicación y abre el enlace que aparece en Pages.

La página recrea la lógica del programa C mediante JavaScript; no ejecuta pthreads en el navegador. No hay pagos reales.
