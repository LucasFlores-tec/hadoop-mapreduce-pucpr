/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.pucpr.operacoes.comerciais;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import java.io.IOException;
import java.util.Collections;
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
public class Informacao2 {
    
    public static class MapperInformacao2 extends Mapper<Object, Text, Text, IntWritable> {
        
        private static final String PAIS = "Brazil";
        
        @Override
        protected void map(Object chave, Text valor, Context context) throws IOException, InterruptedException {
            String linha = valor.toString();
            String[] valores = linha.split(";");
            
            if(valores.length == 10 && PAIS.equals(valores[0])) {
                String mercadoria = valores[3];
                int quantidadeTransacoes = 1;
            
                Text chaveMap = new Text(mercadoria);
                IntWritable valorMap = new IntWritable(quantidadeTransacoes);
                
                context.write(chaveMap, valorMap);
            }
        }
    }
    
    public static class ReducerInformacao2 extends Reducer<Text, IntWritable, Text, IntWritable> {
        
        private Text mercadoria;
        private IntWritable qtdTransacoes;
        
        @Override
        protected void setup(Context context) {
            mercadoria = new Text("");
            qtdTransacoes = new IntWritable(0);
        }
        
        @Override
        protected void reduce(Text chave, Iterable<IntWritable> valores, Context context) {
            int soma = 0;
            
            for(IntWritable valor : valores) {
                soma += valor.get();
            }
            
            if(soma > qtdTransacoes.get()) {
                qtdTransacoes.set(soma);
                mercadoria.set(chave.toString());
            }
        }
        
        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            context.write(mercadoria, qtdTransacoes);
        }
        
    }

    public static void main (String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        
          String arquivoEntrada = "/home/Disciplinas/FundamentosBigData/OperacoesComerciais/base_100_mil.csv";
          String arquivoSaida = "/home2/ead2022/SEM1/lucas.francisconi/Desktop/atp/etapa2/informacao2";
        
          if(args.length == 2) {
            arquivoEntrada = args[0];
            arquivoSaida = args[1];
          }
        
          Configuration conf = new Configuration();
          Job job = Job.getInstance(conf, "etapa02-atv02");
        
          job.setJarByClass(Informacao2.class);
          job.setMapperClass(MapperInformacao2.class);
          job.setReducerClass(ReducerInformacao2.class);
          job.setOutputKeyClass(Text.class);
          job.setOutputValueClass(IntWritable.class);
        
          FileInputFormat.addInputPath(job, new Path(arquivoEntrada));
          FileOutputFormat.setOutputPath(job, new Path(arquivoSaida));
        
          job.waitForCompletion(true);
    }
}
