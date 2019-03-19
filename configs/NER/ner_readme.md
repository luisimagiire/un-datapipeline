
# NER Processing Configs

O processo de encoding dos dados para o modelo NER é mais complexo que o de pricing.

## NER Schema parsing
Primeiro deve-se rodar o **ner.schema.config.gin** para capturar os schemas de propriedades
vindos do novo batch de dados do ML.

Fields para mudar:
* parallel_process.dataset_path
* parallel_process.workers
* valid_advertise.category

O output devera estar na pasta reduced (ou em outra qualquer configurada em 
SchemaReducer.output_folder)


## NER Sequence

O próximo config a rodar é o **sequence.config.gin**, ele vai utilizar
o schema gerado anteriormente para gerar a sequencia de labels a ser encoded.

Exemplo: "iphone 6 32 gb"
Resultado: [['iphone 6', "MODELO"], ["32 GB"," MEMORIA_INTERNA"]]  


Fields para mudar:
* SequenceEncoder.properties_path - JSON schema path
* valid_advertise.category
* parallel_process.dataset_path
* parallel_process.workers


## NER Mapping

Necessário para processar os mappings para o encoding.
Utiliza as pastas de dados do NER sequence para gerar mappings
em pararelo que depois sao agregados em um unico mapping.
Este ultimo será o mapping dos encodings final do modelo.


Fields para mudar:
* parallel_process.dataset_path
* valid_advertise.category
* parallel_process.workers


## NER Encoding

Principal e final config para fazer o encoding
dos dados a serem processados pelo modelo.
O config fará a agregação e entregara uma pasta
com o split de test e train dos dados.

Fields para mudar:
* parallel_process.dataset_path
* parallel_process.workers
* NEREncoder.model_folder - (output folder)
* NEREncoder.maps_folder - (mapping folder)
* NEREncoder.debug
* NERReducer.test_perc - [0, 1) float - percentagem para teste dataset