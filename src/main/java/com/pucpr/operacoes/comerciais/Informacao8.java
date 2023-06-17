/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.pucpr.operacoes.comerciais;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * @author lucas.francisconi
 */
public class Informacao8 {
    
    public static class MapperInformacao8 extends Mapper<Object, Text, Text, Text> {
        
       @Override
        protected void map(Object chave, Text valor, Context context) throws IOException, InterruptedException {
            String linha = valor.toString();
            String[] valores = linha.split(";");
            
            if(valores.length == 10) {
                String ano = valores[1];
                String mercadoriaPeso = valores[3] + ":" + valores[6];
            
                Text chaveMap = new Text(ano);
                Text valorMap = new Text(mercadoriaPeso);
                
                context.write(chaveMap, valorMap);
            }
        }
    }
    
    public static class ReducerInformacao8 extends Reducer<Text, Text, Text, Text> {
        
        @Override
        protected void reduce(Text chave, Iterable<Text> valores, Context context) throws IOException, InterruptedException {
            
            Map<String, Integer> nomePeso = new HashMap<>();
            
            for(Text text : valores) {
                String[] partes = text.toString().split(":");
                if(partes.length == 2) {
                    String nome = partes[0];
                    int peso = Integer.parseInt(partes[1]);
                    int pesoTotal = nomePeso.getOrDefault(nome, 0);
                    nomePeso.put(nome, pesoTotal + peso);
                }
            }
            
            String objetoMaiorPeso = "";
            int maiorPeso = 0;
            
            for(Map.Entry<String, Integer> entry : nomePeso.entrySet()) {
                if(entry.getValue() > maiorPeso) {
                    objetoMaiorPeso = entry.getKey();
                    maiorPeso = entry.getValue();
                }
            }
            
            String res = objetoMaiorPeso + " : " + String.valueOf(maiorPeso);
            
            Text resultado = new Text(res);
            
            context.write(chave, resultado);
        }
    
    }

    public static void main (String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        
          String arquivoEntrada = "/home/Disciplinas/FundamentosBigData/OperacoesComerciais/base_100_mil.csv";
          String arquivoSaida = "/home2/ead2022/SEM1/lucas.francisconi/Desktop/atp/etapa2/informacao8";
        
          if(args.length == 2) {
            arquivoEntrada = args[0];
            arquivoSaida = args[1];
          }
        
          Configuration conf = new Configuration();
          Job job = Job.getInstance(conf, "etapa02-atv08");
        
          job.setJarByClass(Informacao8.class);
          job.setMapperClass(MapperInformacao8.class);
          job.setReducerClass(ReducerInformacao8.class);
          job.setOutputKeyClass(Text.class);
          job.setOutputValueClass(Text.class);
        
          FileInputFormat.addInputPath(job, new Path(arquivoEntrada));
          FileOutputFormat.setOutputPath(job, new Path(arquivoSaida));
        
          job.waitForCompletion(true);
    }
}
