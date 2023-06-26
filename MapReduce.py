import os
import multiprocessing
import string
import time
from collections import defaultdict
import matplotlib.pyplot as plt

def mapper(file):
    word_counts = defaultdict(int)
    total_words = 0

    with open(file, 'r', encoding='latin-1') as f:
        for line in f:
            line = line.strip().lower()
            words = line.translate(str.maketrans('', '', string.punctuation)).split()

            for word in words:
                word_counts[word] += 1
                total_words += 1

    return word_counts, total_words

def reducer(results_list):
    word_counts = defaultdict(int)
    total_words = 0

    for word_count, count in results_list:
        for word, word_count in word_count.items():
            word_counts[word] += word_count
        total_words += count

    return word_counts, total_words

def get_files(directory):
    file_list = []

    for root, _, files in os.walk(directory):
        for file in files:
            file_path = os.path.join(root, file)
            file_list.append(file_path)

    return file_list

def process_data(percentage, num_processes, file_names):
    num_files = int(len(file_names) * percentage)
    files_to_process = file_names[:num_files]

    pool = multiprocessing.Pool(processes=num_processes)
    results = pool.map(mapper, files_to_process)

    pool.close()
    pool.join()

    word_counts, total_words = reducer(results)

    return word_counts, total_words

def main():
    data_path = '/path/to/Gutenberg_Text-master'  # Ruta donde se encuentra la carpeta Gutenberg_Text-master
    file_names = get_files(data_path)

    percentages = [0.25, 0.5, 1.0]  # Porcentajes del conjunto de datos a utilizar
    num_processors = os.cpu_count()  # Número de procesadores disponibles
    max_processes = min(5, num_processors)  # Máximo 5 procesos

    for percentage in percentages:
        num_files = int(len(file_names) * percentage)
        files_to_process = file_names[:num_files]

        time_data = []  # Lista para almacenar los tiempos de procesamiento
        words_data = []  # Lista para almacenar el total de palabras contadas
        process_data_list = []  # Lista para almacenar el número de procesos utilizados

        for num_processes in range(1, max_processes + 1):
            # Procesar los datos y obtener el tiempo de procesamiento y el total de palabras contadas
            start_time = time.time()
            word_counts, total_words = process_data(percentage, num_processes, files_to_process)
            end_time = time.time()

            # Guardar los resultados en un archivo de texto
            output_file = f"results/results_{int(percentage * 100)}percent_{num_processes}processes.txt"
            with open(output_file, 'w') as f:
                f.write(f"Porcentaje del conjunto de datos: {percentage * 100}%\n")
                f.write(f"Número de procesos utilizados: {num_processes}\n")
                f.write(f"Tiempo de procesamiento: {end_time - start_time:.2f} segundos\n")
                f.write(f"Total de palabras contadas: {total_words}\n")
                f.write("------------------------------\n")

            # Imprimir los resultados en la salida
            print(f"Porcentaje del conjunto de datos: {percentage * 100}%")
            print(f"Número de procesos utilizados: {num_processes}")
            print(f"Tiempo de procesamiento: {end_time - start_time:.2f} segundos")
            print(f"Total de palabras contadas: {total_words}")
            print("------------------------------")

            # Guardar los tiempos, palabras contadas y número de procesos utilizados en las listas
            time_data.append(end_time - start_time)
            words_data.append(total_words)
            process_data_list.append(num_processes)

        # Generar el gráfico
        plt.figure(figsize=(10, 6))
        plt.plot(process_data_list, time_data, marker='o', label='Tiempo de Procesamiento')
        plt.plot(process_data_list, words_data, marker='o', label='Total de Palabras')
        plt.xlabel('Número de Procesos')
        plt.ylabel('Valor')
        plt.legend()
        plt.title(f'Rendimiento del Contador de Palabras ({int(percentage * 100)}% del dataset)')
        plt.savefig(f'grafico_{int(percentage * 100)}percent.png')  # Guardar el gráfico en un archivo de imagen
        plt.close()

if __name__ == '__main__':
    main()
