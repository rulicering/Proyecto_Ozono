# coding: utf-8

"""
    AEMET OpenData

    AEMET OpenData es una API REST desarrollado por AEMET que permite la difusión y la reutilización de la información meteorológica y climatológica de la Agencia, en el sentido indicado en la Ley 18/2015, de 9 de julio, por la que se modifica la Ley 37/2007, de 16 de noviembre, sobre reutilización de la información del sector público. (IMPORTANTE: Para poder realizar peticiones, es necesario introducir en API Key haciendo clic en el círculo rojo de recurso REST).  # noqa: E501

    OpenAPI spec version: 2.0
    
    Generated by: https://github.com/swagger-api/swagger-codegen.git
"""


from __future__ import absolute_import

import unittest

import swagger_client
from swagger_client.api.predicciones_especificas_api import PrediccionesEspecificasApi  # noqa: E501
from swagger_client.rest import ApiException


class TestPrediccionesEspecificasApi(unittest.TestCase):
    """PrediccionesEspecificasApi unit test stubs"""

    def setUp(self):
        self.api = swagger_client.api.predicciones_especificas_api.PrediccionesEspecificasApi()  # noqa: E501

    def tearDown(self):
        pass

    def test_informacion_nivologica_(self):
        """Test case for informacion_nivologica_

        Información nivológica.  # noqa: E501
        """
        pass

    def test_prediccin_de_montaa__tiempo_actual_(self):
        """Test case for prediccin_de_montaa__tiempo_actual_

        Predicción de montaña. Tiempo actual.  # noqa: E501
        """
        pass

    def test_prediccin_de_montaa__tiempo_pasado_(self):
        """Test case for prediccin_de_montaa__tiempo_pasado_

        Predicción de montaña. Tiempo pasado.  # noqa: E501
        """
        pass

    def test_prediccin_de_radiacin_ultravioleta__uvi_(self):
        """Test case for prediccin_de_radiacin_ultravioleta__uvi_

        Predicción de radiación ultravioleta (UVI).  # noqa: E501
        """
        pass

    def test_prediccin_para_las_playas__tiempo_actual_(self):
        """Test case for prediccin_para_las_playas__tiempo_actual_

        Predicción para las playas. Tiempo actual.  # noqa: E501
        """
        pass

    def test_prediccin_por_municipios_diaria__tiempo_actual_(self):
        """Test case for prediccin_por_municipios_diaria__tiempo_actual_

        Predicción por municipios diaria. Tiempo actual.  # noqa: E501
        """
        pass

    def test_prediccin_por_municipios_horaria__tiempo_actual_(self):
        """Test case for prediccin_por_municipios_horaria__tiempo_actual_

        Predicción por municipios horaria. Tiempo actual.  # noqa: E501
        """
        pass


if __name__ == '__main__':
    unittest.main()