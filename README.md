# Introdução
==============

[![Join the chat at https://gitter.im/linkedin/databus](https://badges.gitter.im/Join%20Chat.svg)](https://gitter.im/linkedin/databus?utm_source=badge&utm_medium=badge&utm_campaign=pr-badge&utm_content=badge)

Nas arquiteturas da Internet, os sistemas de dados são normalmente categorizados em sistemas de fonte da verdade que servem como armazenamentos primários para as gravações geradas pelo usuário e armazenamentos de dados derivados ou índices que servem para leituras e outras consultas complexas. Os dados nesses armazenamentos secundários geralmente são derivados dos dados primários por meio de transformações personalizadas, às vezes envolvendo processamento complexo orientado pela lógica de negócios. Da mesma forma, os dados nas camadas de cache são derivados de leituras no armazenamento de dados primário, mas precisam ser invalidados ou atualizados quando os dados primários são modificados. Um requisito fundamental que emerge desses tipos de arquiteturas de dados é a necessidade de capturar, fluir e processar alterações de dados primários de forma confiável.

Construímos o Databus, um sistema de captura de dados de alteração distribuído independente de fonte, que é parte integrante do pipeline de processamento de dados do LinkedIn. A camada de transporte do Databus fornece latências em poucos milissegundos e lida com a taxa de transferência de milhares de eventos por segundo por servidor, ao mesmo tempo em que oferece suporte a recursos infinitos de retrospectiva e a uma rica funcionalidade de assinatura.

# Casos de uso
*****
Normalmente, os armazenamentos de dados OLTP primários recebem gravações e algumas leituras voltadas para o usuário, enquanto outros sistemas especializados atendem a consultas complexas ou aceleram os resultados da consulta por meio do armazenamento em cache. Os sistemas de dados mais comuns encontrados nessas arquiteturas incluem bancos de dados relacionais, armazenamentos de dados NoSQL, mecanismos de cache, índices de pesquisa e mecanismos de consulta de gráficos. Essa especialização, por sua vez, tornou essencial ter um pipeline de dados confiável e escalável que possa capturar essas mudanças que ocorrem nos sistemas primários de fonte da verdade e encaminhá-los para o restante do complexo ecossistema de dados. Existem duas famílias de soluções que normalmente são usadas para construir esse pipeline.

### Gravações duplas orientadas por aplicativos:
Nesse modelo, a camada de aplicativo grava no banco de dados e, paralelamente, grava em outro sistema de mensagens. Isso parece simples de implementar, pois a gravação do código do aplicativo no banco de dados está sob nosso controle. No entanto, introduz um problema de consistência porque sem um protocolo de coordenação complexo (por exemplo, Paxos ou 2-Phase Commit ), é difícil garantir que o banco de dados e o sistema de mensagens estejam em total bloqueio entre si em caso de falhas. Ambos os sistemas precisam processar exatamente as mesmas gravações e serializá-las exatamente na mesma ordem. As coisas ficam ainda mais complexas se as gravações forem condicionais ou tiverem semântica de atualização parcial.

### Mineração de logs de banco de dados: 
Neste modelo, tornamos o banco de dados a única fonte da verdade e extraímos as alterações de sua transação ou log de confirmação.Isso resolve nosso problema de consistência, mas é praticamente difícil de implementar porque bancos de dados como Oracle e MySQL (os principais armazenamentos de dados em uso no LinkedIn) têm formatos de log de transações e soluções de replicação que são proprietárias e não garantem ter
representações estáveis em disco ou on-the-wire em atualizações de versão. Como queremos processar as alterações de dados com o código do aplicativo e, em seguida, gravar em armazenamentos de dados secundários, precisamos que o sistema de replicação seja de espaço de usuário e independente de fonte. Essa independência da fonte de dados é especialmente importante em empresas de tecnologia em rápida evolução, porque evita o bloqueio da tecnologia e a vinculação a formatos binários em toda a pilha de aplicativos.

Depois de avaliar os prós e os contras das duas abordagens, decidimos buscar a opção de mineração de log, priorizando consistência e "fonte única de verdade" sobre a facilidade de implementação. Neste artigo, apresentamos o Databus, pipeline de Change Data Capture no LinkedIn, que suporta fontes Oracle e uma ampla gama de aplicativos downstream. O Social Graph Index, que atende a todas as consultas gráficas no LinkedIn, o People Search Index, que alimenta todas as pesquisas de membros no LinkedIn e as várias réplicas de leitura dos dados do perfil do membro, são todos alimentados e mantidos consistentes via Databus.

Mais detalhes sobre a arquitetura, casos de uso e avaliação de desempenho podem ser obtidos em um artigo aceito para publicação no ACM Symposium on Cloud Computing - 2012. Os slides da apresentação estão disponíveis [aqui](http://www.slideshare. net/ShirshankaDas/databus-socc-2012)

# How to build ?
*****
Databus requires a library distributed by Oracle Inc under Oracle Technology Network License. Please accept that license [here](http://www.oracle.com/technetwork/licenses/distribution-license-152002.html), and download ojdbc6.jar with version at 11.2.0.2.0 [here](http://www.oracle.com/technetwork/database/enterprise-edition/jdbc-112010-090769.html). Once you download the driver jar, please copy it under sandbox-repo/com/oracle/ojdbc6/11.2.0.2.0/ and name it ojdbc6-11.2.0.2.0.jar as shown below. We have provided a sample .ivy file to facilitate the build.

Databus will **NOT** build without this step. After downloading the jars, they may be copied under the directory sandbox-repo as :
* sandbox-repo/com/oracle/ojdbc6/11.2.0.2.0/ojdbc6-11.2.0.2.0.jar
* sandbox-repo/com/oracle/ojdbc6/11.2.0.2.0/ojdbc6-11.2.0.2.0.ivy

# Build System
*****
Databus currently needs gradle version 1.0 or above to build. The commands to build are :
* `gradle -Dopen_source=true assemble` -- builds the jars and command line package
* `gradle -Dopen_source=true clean`    -- cleans the build directory
* `gradle -Dopen_source=true test`     -- runs all the unit-tests that come packaged with the source

# Licensing
*****
Databus will be licensed under Apache 2.0 license.

# Full Documentation
*****
See our [wiki](https://github.com/linkedin/databus/wiki) for full documentation and examples.

# Example Relay
*****
An example of writing a DatabusRelay is available at PersonRelayServer.java. To be able to start a relay process, the code is packaged into a startable command-line package. The tarball may be obtained from build/databus2-example-relay-pkg/distributions/databus2-example-relay-pkg.tgz. This relay is configured to get changestreams for a view "Person".

After extracting to a directory, please cd to that directory and start the relay using the following command :
* `./bin/start-example-relay.sh person`

If the relay is started successfully, the output of the following curl command would look like :
* $ `curl http://localhost:11115/sources`
* `[{“name”:“com.linkedin.events.example.person.Person”,“id”:40}]`

# Example Client
*****
An example of writing a DatabusClient is available at PersonClientMain.java. To easily be able to start the client process, the code is packaged into a startable command-line package. The tarball may be obtained from build/databus2-example-client-pkg/distributions/databus2-example-client-pkg.tgz. This client is configured to get data from the relay started previously, and configured to susbscribe for table Person.

After extracting to a directory, please cd to that directory and start the client using the following command :
* `./bin/start-example-client.sh person`

If the client successfully connects to the relay we created earlier, the output of the following curl command would look like below ( indicating a client from localhost has connected to the relay ):
* $`curl http://localhost:11115/relayStats/outbound/http/clients`
* `["localhost"]`
