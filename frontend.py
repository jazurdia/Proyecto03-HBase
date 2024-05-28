from tkinter import *
import ejecucion_comandos as ec
import customtkinter as ctk


class App(ctk.CTk):
    def __init__(self):
        super().__init__()
        
        # Configuración de la ventana
        self.title("HBase Clone")
        self.geometry("800x600")
        
        # Configuración del tema
        ctk.set_appearance_mode("dark")  # Opciones: "light", "dark", "system"
        ctk.set_default_color_theme("blue")  # Opciones: "blue", "green", "dark-blue"
        
        # Crear y colocar widgets
        self.create_widgets()
        
    def create_widgets(self):
        # Etiqueta de bienvenida
        self.label_bienvenida = ctk.CTkLabel(self, text="Bienvenido a HBase Clone!")
        self.label_bienvenida.pack(pady=10)
        
        # Área de texto para mostrar resultados (solo lectura)
        self.text_area = ctk.CTkTextbox(self, width=600, height=300, activate_scrollbars=True)
        self.text_area.pack(pady=10)
        self.text_area.configure(state='disabled')  # Hacer el área de texto de solo lectura
        
        # Etiqueta para el campo de entrada
        self.label_entrada = ctk.CTkLabel(self, text="Ingrese un comando:")
        self.label_entrada.pack()
        
        # Campo de entrada para comandos
        self.entrada_comando = ctk.CTkTextbox(self, activate_scrollbars=True, width=600, height=50)
        self.entrada_comando.pack(pady=10)   
        
        # Frame para los botones
        self.frame_botones = ctk.CTkFrame(self)
        self.frame_botones.pack(pady=10)
        
        # Botón para ejecutar comando
        self.boton_ejecutar = ctk.CTkButton(self.frame_botones, text="Ejecutar", command=self.ejecutar_comando)
        self.boton_ejecutar.pack(side=LEFT, padx=5)
        
        # Botón para borrar resultados en shell
        self.boton_borrar_resultados = ctk.CTkButton(self.frame_botones, text="Borrar Resultados", command=self.borrar_resultados)
        self.boton_borrar_resultados.pack(side=LEFT, padx=5)
        
        # Botón para borrar campo de entrada
        self.boton_borrar_comando = ctk.CTkButton(self.frame_botones, text="Borrar Comando", command=self.borrar_comando)
        self.boton_borrar_comando.pack(side=LEFT, padx=5)
        
    def ejecutar_comando(self):
        comando = self.entrada_comando.get("1.0", END).strip()
        resultado = f"Ejecutando: {comando}\n"  # Simulación del resultado del comando
        exec_result, mensaje = ec.identificar_comando(comando)
        if exec_result:
            resultado += mensaje + "\n"
        else:
            resultado += mensaje + "\n"      
        
        # Habilitar el área de texto, insertar el resultado y volver a deshabilitar
        self.text_area.configure(state='normal')
        self.text_area.insert(END, resultado)
        self.text_area.configure(state='disabled')
        
    def borrar_resultados(self):
        # Habilitar el área de texto, borrar el contenido y volver a deshabilitar
        self.text_area.configure(state='normal')
        self.text_area.delete('1.0', END)
        self.text_area.configure(state='disabled')
        
    def borrar_comando(self):
        self.entrada_comando.delete('1.0', END)


if __name__ == '__main__':
    app = App()
    app.mainloop()
