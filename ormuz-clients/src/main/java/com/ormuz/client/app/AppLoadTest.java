package com.ormuz.client.app;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import com.ormuz.client.core.Client;
import com.ormuz.shared.enums.ServicesTypes;

/**
 * Script de Teste de Consistência sob Carga (Requisito Barema).
 * Comprova o funcionamento da Exclusão Mútua Distribuída e da
 * Fila por Ordem de Chegada enviando dezenas de mensagens simultâneas.
 */
public class AppLoadTest extends Client {

    public static void main(String[] args) throws InterruptedException {
        String ipBroker = args.length > 0 ? args[0] : "127.0.0.1";
        int portaBroker = 8080;
        String setor    = "SETOR_TESTE_CARGA";
        int totalRequests = 100;

        AppLoadTest tester = new AppLoadTest();
        tester.setId("STRESS-TESTER");
        tester.setSectorId(setor);

        System.out.println("==================================================");
        System.out.println(" INICIANDO TESTE DE CARGA DE REQUISIÇÕES ORMUZ");
        System.out.println("==================================================");
        System.out.println("Conectando aos canais de alerta...");

        tester.startPublisher(ipBroker, portaBroker, ServicesTypes.INTRUSION_DETECTION);
        tester.startPublisher(ipBroker, portaBroker, ServicesTypes.SEARCH_AND_RESCUE);

        // Aguarda 3 segundos para garantir que o broker registre os sockets
        Thread.sleep(3000);

        System.out.println("\n[TESTE] Disparando " + totalRequests + " requisições simultâneas...");
        
        // Usa um Pool de Threads para enviar mensagens concorrentemente de verdade
        ExecutorService executor = Executors.newFixedThreadPool(20);

        for (int i = 1; i <= totalRequests; i++) {
            final int requestId = i;
            executor.submit(() -> {
                // Alterna os tipos de serviços apenas para gerar tráfego misto
                if (requestId % 2 == 0) {
                    tester.sendMessage(1, ServicesTypes.SEARCH_AND_RESCUE);
                } else {
                    tester.sendMessage(1, ServicesTypes.INTRUSION_DETECTION);
                }
            });
        }

        executor.shutdown();
        executor.awaitTermination(10, TimeUnit.SECONDS);

        System.out.println("\n[TESTE] Todas as requisições enviadas ao barramento.");
        System.out.println("[TESTE] Verifique os logs do Broker (docker attach broker-norte)!");
        System.out.println("[TESTE] Você deverá ver:");
        System.out.println("   1. Drones disponíveis sendo ativados uma única vez (Exclusão Mútua).");
        System.out.println("   2. Dezenas de mensagens informando que as ocorrências foram enviadas para a FILA DISTRIBUÍDA.");
        System.out.println("   3. Assim que um drone terminar (15s), ele desempilhará a ocorrência mais antiga (Ordem de Chegada).");
        
        System.out.println("\nFinalizando processo de teste em 5 segundos...");
        Thread.sleep(5000);
        System.exit(0);
    }
}