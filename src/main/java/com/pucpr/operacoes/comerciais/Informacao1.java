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
public class Informacao1 {
    
    public static class MapperInformacao1 extends Mapper<Object, Text, Text, IntWritable> {
        
        @Override
        protected void map(Object chave, Text valor, Context context) throws IOException, InterruptedException {
            String linha = valor.toString();
            String[] valores = linha.split(";");
            
            if(valores.length == 10) {
                String pais = valores[0];
                int qtdTransacoes = 1;
            
                Text chaveMap = new Text(pais);
                IntWritable valorMap = new IntWritable(qtdTransacoes);
                
                context.write(chaveMap, valorMap);
            }
        }
    }
    
    public static class ReducerInformacao1 extends Reducer<Text, IntWritable, Text, IntWritable> {
        
        private Text pais;
        private IntWritable qtdTransactions;
        
        @Override
        protected void setup(Context context) {
            pais = new Text("");
            qtdTransactions = new IntWritable(0);
        }
        
        @Override
        protected void reduce(Text chave, Iterable<IntWritable> valores, Context context) {
            int soma = 0;
            
            for(IntWritable valor : valores) {
                soma += valor.get();
            }
            
            if(soma > qtdTransactions.get()) {
                qtdTransactions.set(soma);
                pais.set(chave.toString());
            }
        }
        
        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            context.write(pais, qtdTransactions);
        }
        
    }

    public static void main (String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        
          String arquivoEntrada = "/home/Disciplinas/FundamentosBigData/OperacoesComerciais/base_100_mil.csv";
          String arquivoSaida = "/home2/ead2022/SEM1/lucas.francisconi/Desktop/atp/etapa2/informacao1";
        
          if(args.length == 2) {
            arquivoEntrada = args[0];
            arquivoSaida = args[1];
          }
        
          Configuration conf = new Configuration();
          Job job = Job.getInstance(conf, "etapa02-atv01");
        
          job.setJarByClass(Informacao1.class);
          job.setMapperClass(MapperInformacao1.class);
          job.setReducerClass(ReducerInformacao1.class);
          job.setOutputKeyClass(Text.class);
          job.setOutputValueClass(IntWritable.class);
        
          FileInputFormat.addInputPath(job, new Path(arquivoEntrada));
          FileOutputFormat.setOutputPath(job, new Path(arquivoSaida));
        
          job.waitForCompletion(true);
    }
}
