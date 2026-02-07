# -*- coding: utf-8 -*-
# file: anti_fraud.py (o src/anti_fraud.py)
import hashlib
import logging

logger = logging.getLogger("AntiFraud")

class AntiFraudSystem:
    """
    Sistema de Auditoría Pasiva.
    Calcula huellas digitales (HASH MD5) de las imágenes para detectar duplicados
    sin molestar al vendedor.
    """

    @staticmethod
    def calculate_hash(image_bytes: bytearray) -> str:
        """Genera el hash MD5 único de una imagen."""
        try:
            hash_md5 = hashlib.md5()
            hash_md5.update(image_bytes)
            return hash_md5.hexdigest()
        except Exception as e:
            logger.error(f"Error calculando hash: {e}")
            return "error_hash"

    @staticmethod
    def check_duplicate(current_hash: str, historical_hashes: list) -> bool:
        """
        Compara el hash actual contra la lista histórica.
        Retorna True si es duplicado (FRAUDE POTENCIAL).
        """
        if not current_hash or current_hash == "error_hash":
            return False
            
        return current_hash in historical_hashes