import os
import pickle
import json
import tempfile
import uuid
from typing import Optional
from googleapiclient.discovery import build
from googleapiclient.discovery import build
from google.oauth2.service_account import Credentials
from googleapiclient.http import MediaFileUpload
from googleapiclient.errors import HttpError
import time

class DriveManager:
    """Clase para manejar la API de Google Drive"""
    
    SCOPES = ['https://www.googleapis.com/auth/drive']
    
    def __init__(self, service_account_path: str = 'credentials-service.json'):
        self.service_account_path = service_account_path
        self.service = None
        self._folder_cache = {}
    
    def authenticate(self):
        """Autentica usando Service Account (sin intervención del usuario)"""
        try:
            if not os.path.exists(self.service_account_path):
                raise FileNotFoundError(f"Archivo de Service Account no encontrado: {self.service_account_path}")
            
            # Cargar credenciales desde el archivo JSON
            creds = Credentials.from_service_account_file(
                self.service_account_path, scopes=self.SCOPES)
            
            # Crear servicio
            self.service = build('drive', 'v3', credentials=creds)
            
            # Validar conexión
            self.service.about().get(fields="user").execute()
            print("✅ Autenticación con Service Account exitosa")
            return self.service
            
        except Exception as e:
            print(f"❌ Error autenticando con Service Account: {e}")
            raise
    
    def validate_connection(self) -> bool:
        """Valida que la conexión sea válida"""
        try:
            if not self.service:
                self.authenticate()
            
            self.service.about().get(fields="user,storageQuota").execute()
            return True
        except Exception as e:
            print(f"❌ Conexión inválida: {e}")
            return False   

    def get_folder_id(self, folder_path: str, create_if_not_exists: bool = True) -> Optional[str]:
        """
        Obtiene el ID de una carpeta por su ruta (ej: 'ARTICULOS JSON')
        Si create_if_not_exists=True, crea la carpeta si no existe
        """
        # Usar cache si ya se buscó esta carpeta
        if folder_path in self._folder_cache:
            return self._folder_cache[folder_path]
        
        try:
            # Buscar carpeta en Mi unidad
            query = f"name='{folder_path}' and mimeType='application/vnd.google-apps.folder' and trashed=false"
            results = self.service.files().list(q=query, fields="files(id, name)").execute()
            folders = results.get('files', [])
            
            if folders:
                folder_id = folders[0]['id']
                self._folder_cache[folder_path] = folder_id
                print(f"📁 Carpeta encontrada: {folder_path}")
                return folder_id
            
            elif create_if_not_exists:
                # Crear carpeta si no existe
                folder_metadata = {
                    'name': folder_path,
                    'mimeType': 'application/vnd.google-apps.folder'
                }
                folder = self.service.files().create(body=folder_metadata, fields='id').execute()
                folder_id = folder.get('id')
                self._folder_cache[folder_path] = folder_id
                print(f"📁 Carpeta creada: {folder_path}")
                return folder_id
            
            return None
            
        except HttpError as error:
            print(f"❌ Error buscando/creando carpeta {folder_path}: {error}")
            return None
    
    def upload_json_data(self, data: list, filename: str, folder_path: str) -> bool:
        """
        Sube datos JSON directamente a Google Drive
        """
        if not self.service:
            print("❌ Servicio no autenticado")
            return False
        
        temp_file = None
        try:
            # Obtener ID de la carpeta
            folder_id = self.get_folder_id(folder_path)
            if not folder_id:
                print(f"❌ No se pudo obtener/crear la carpeta: {folder_path}")
                return False
            
            # Crear archivo temporal con nombre único
            temp_file = tempfile.NamedTemporaryFile(
                mode='w', 
                suffix='.json', 
                prefix=f'drive_upload_{uuid.uuid4().hex[:8]}_',
                delete=False,  # No eliminar automáticamente
                encoding='utf-8'
            )
            
            # Escribir datos JSON al archivo temporal
            json.dump(data, temp_file, ensure_ascii=False, indent=2)
            temp_file.flush()  # Asegurar que se escriban los datos
            temp_file.close()  # Cerrar el archivo para que Windows pueda accederlo
            
            # Verificar si el archivo ya existe para actualizarlo
            existing_file_id = self._get_file_id_in_folder(filename, folder_id)
            
            # Preparar metadatos y media
            media = MediaFileUpload(
                temp_file.name,  # Usar el nombre del archivo temporal
                mimetype='application/json',
                resumable=True,
                chunksize=1024*1024*8  # 8MB chunks
            )
            
            if existing_file_id:
                # Actualizar archivo existente
                print(f"🔄 Actualizando archivo existente: {filename}")
                request = self.service.files().update(
                    fileId=existing_file_id,
                    media_body=media
                )
            else:
                # Crear nuevo archivo
                print(f"📤 Creando nuevo archivo: {filename}")
                file_metadata = {
                    'name': filename,
                    'parents': [folder_id]
                }
                request = self.service.files().create(
                    body=file_metadata,
                    media_body=media,
                    fields='id'
                )
            
            # Ejecutar upload con retry y progress
            response = self._execute_upload_with_retry(request, filename)
            
            if response:
                print(f"✅ Archivo {filename} subido exitosamente a {folder_path}")
                return True
            else:
                print(f"❌ Error subiendo {filename}")
                return False
                
        except Exception as e:
            print(f"❌ Error en upload_json_data: {e}")
            return False
        
        finally:
            # Limpiar archivo temporal de forma segura
            if temp_file and os.path.exists(temp_file.name):
                try:
                    os.unlink(temp_file.name)  # Eliminar archivo temporal
                except Exception as cleanup_error:
                    print(f"⚠️ Advertencia: No se pudo eliminar archivo temporal: {cleanup_error}")
    
    def _get_file_id_in_folder(self, filename: str, folder_id: str) -> Optional[str]:
        """Busca un archivo por nombre dentro de una carpeta específica"""
        try:
            query = f"name='{filename}' and '{folder_id}' in parents and trashed=false"
            results = self.service.files().list(q=query, fields="files(id, name)").execute()
            files = results.get('files', [])
            return files[0]['id'] if files else None
        except HttpError:
            return None
    
    def _execute_upload_with_retry(self, request, filename: str, max_retries: int = 3):
        """Ejecuta upload con reintentos y manejo de progreso"""
        for attempt in range(max_retries):
            try:
                response = None
                while response is None:
                    status, response = request.next_chunk()
                    if status:
                        progress = int(status.progress() * 100)
                        print(f"📊 Progreso {filename}: {progress}%")
                
                return response
                
            except HttpError as error:
                if error.resp.status == 429:  # Rate limit
                    wait_time = (2 ** attempt) + 1
                    print(f"⏳ Rate limit alcanzado. Esperando {wait_time}s...")
                    time.sleep(wait_time)
                elif error.resp.status >= 500:  # Server errors
                    wait_time = (2 ** attempt) + 1
                    print(f"⏳ Error servidor. Reintentando en {wait_time}s...")
                    time.sleep(wait_time)
                else:
                    print(f"❌ Error HTTP: {error}")
                    break
            except Exception as e:
                print(f"❌ Error en intento {attempt + 1}: {e}")
                if attempt < max_retries - 1:
                    time.sleep(2 ** attempt)
        
        return None
    
    def test_connection(self) -> bool:
        """Prueba la conexión con Google Drive"""
        try:
            if not self.service:
                self.authenticate()
            
            # Hacer una consulta simple
            results = self.service.files().list(pageSize=1).execute()
            print("✅ Conexión con Google Drive exitosa")
            return True
            
        except Exception as e:
            print(f"❌ Error conectando con Google Drive: {e}")
            return False