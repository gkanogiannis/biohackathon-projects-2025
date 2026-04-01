# buscophy pipeline: Generation of phylogenetic trees from busco genes

**This is a work in progress, the workflow is still at a testing stage**

### Workflow Goal
This workflow is a snakemake workflow for benchmarking BUSCO genes in phylogenomics, designed to assess and improve the accuracy of phylogenetic reconstructions by systematically identifying and accounting for paralogy in BUSCO datasets. It automates the detection of in- and out-paralogs, filters problematic loci, and generates robust phylogenies using concatenated maximum likelihood and coalescent methods. The pipeline is built to be portable, FAIR-compliant, and adaptable for both amino acid and nucleotide datasets, making it suitable for diverse taxonomic groups and research needs. By addressing the challenge of paralogy in BUSCO-based analyses, it strengthens the reliability of phylogenomic studies for researchers, genome consortia, and bioinformaticians. The workflow integrates seamlessly into existing genomics pipelines and is openly accessible to the scientific community.


### Workflow origin
- The main snakemake workflow comes from https://gitlab.leibniz-lib.de/smartin/buscophy written by [Sebastian Martin](https://gitlab.leibniz-lib.de/smartin)
- Additional python code '[Paralogous_filtering.py](https://github.com/KulyaNikita/biohackathon-projects-2025/blob/paralogy/03-automatic-workflow-for-benchmarking/buscophy/scripts/Paralogous_filtering.py)' from [Nikita Kulikov](https://github.com/KulyaNikita) was included
- Inclusion of the python script in the snakemake workflow, testing and inclusion of the workflow in Galaxy was done during the BioHackathon 2026 by the contributors indicated at the bottom.


### The main changes from the original snakefile are :
* Updated BUSCO v5 to v6 to be able to incorporate the odb12 databases. 
* Imposing use of --metaeuk to get both nt and aa files
* switch to using mafft for alignment of both aa and nt files
* Change the workflow so that individual gene trees are made instead of a tree based on one supermatrix (could be an option)
* Add multi-copy genes and implement a procedure for choosing the paralogs


### The next updates will include :
* Add the option to run busco with the `--augustus` argument. There needs to be some change with the augustus config in the singularity image
* Modularise the snakemake pipeline to help tidy it up and allow for plug-and-play configuration
* Tidy up the conda envs - at the moment these have no version numbers. They should be consistent with the singularity/docker images.
* More testing
* Making the workflow available in Galaxy


### Run the pipeline:

1. [Clone this repository](https://docs.github.com/en/repositories/creating-and-managing-repositories/cloning-a-repository)
`snakemake --use-conda --cores 40 --resources write_slots=1`

`snakemake --cores 40 --resources write_slots=1 --use-singularity --singularity-args="--cleanenv --no-home --bind /path/to/cwd/"`

2. Download and organized the test input data.
   
The input data are genomes in a fasta format (uncompressed) named with the `.fas` extension
The input genomes should be placed in a specific folder indicated in the `config.yaml`.

We recommend you to test the workflow with the following test data (publicly available):

```
mkdir input_test
cd input_test/

# download some lepidoptera genomes
# https://lepbase.cog.sanger.ac.uk/archive/v4/
wget "https://lepbase.cog.sanger.ac.uk/archive/v4/sequence/Bicyclus_anynana_v1.2_-_scaffolds.fa.gz"
wget "https://lepbase.cog.sanger.ac.uk/archive/v4/sequence/Bombyx_mori_ASM15162v1_-_scaffolds.fa.gz"
wget "https://lepbase.cog.sanger.ac.uk/archive/v4/sequence/Callimorpha_dominula_k41_-_scaffolds.fa.gz"
wget "https://lepbase.cog.sanger.ac.uk/archive/v4/sequence/Calycopis_cecrops_v1.1_-_scaffolds.fa.gz"
wget "https://lepbase.cog.sanger.ac.uk/archive/v4/sequence/Glyphotaelius_pellucidus_k51_-_scaffolds.fa.gz"
wget https://ftp.ncbi.nlm.nih.gov/genomes/all/GCA/038/095/585/GCA_038095585.1_ASM3809558v1/GCA_038095585.1_ASM3809558v1_genomic.fna.gz
wget https://ftp.ncbi.nlm.nih.gov/genomes/all/GCF/032/362/555/GCF_032362555.1_ilAmyTran1.1/GCF_032362555.1_ilAmyTran1.1_genomic.fna.gz
wget https://ftp.ncbi.nlm.nih.gov/genomes/all/GCA/963/853/865/GCA_963853865.1_ilHelHell1.1/GCA_963853865.1_ilHelHell1.1_genomic.fna.gz

gunzip *gz

#rename genome assemblies
mv Bicyclus_anynana_v1.2_-_scaffolds.fa Bicyclus_anynana.fas
mv Bombyx_mori_ASM15162v1_-_scaffolds.fa Bombyx_mori.fas 
mv Callimorpha_dominula_k41_-_scaffolds.fa Callimorpha_dominula.fas 
mv Calycopis_cecrops_v1.1_-_scaffolds.fa Calycopis_cecrops.fas 
mv Glyphotaelius_pellucidus_k51_-_scaffolds.fa Glyphotaelius_pellucidus.fas
mv GCA_038095585.1_ASM3809558v1_genomic.fna Grapholita_dimorpha.fas
mv GCF_032362555.1_ilAmyTran1.1_genomic.fna Amyelois_transitella.fas
mv GCA_963853865.1_ilHelHell1.1_genomic.fna Helleia_helle.fas
```

3. Update parameters in the `config.yaml`
   
At the moment all parameters should be changed in the `config.yaml` file including the input directory, busco lineage, minimum number of species and genes and maximum number of threads per job.

Exemple of a config file:

```
input_dir: 'input_test'
lineage: 'lepidoptera_odb12'
fragmented: 'no'
min_spec: 4
min_genes: 3
max_threads: 40
busco_args: "--skip_bbtools" #default: --metaeuk to get both aa and nt files. Would also like to include --augustus
```

You can use this workflow with conda or singularity / apptainer.

#### If you are running it with Conda :

4a. Create base conda environment 
   
`conda env create --file base_snakemake.yml`

4a. Activate base conda environment

`conda activate $(grep "name:" base_snakemake.yml | awk '{print $2}')`

5a. Run the worflow
   
`snakemake --use-conda --cores 40 --resources write_slots=1`

#### If you are using it with singularity / apptainer :

5b. Run the workflow
   
`snakemake --cores 40 --resources write_slots=1 --use-singularity --singularity-args="--cleanenv --no-home --bind /path/to/cwd/"`
Make sure to update the path to your current working directory (cwd) in the command line above


## Contributors

The following people contributed during the 2026 BioHackathon:
Here is the table in Markdown format, excluding the email addresses:

| Name                     | ORCiD                  |
|--------------------------|------------------------|
| Tereza Manousaki         | 0000-0001-7518-0542    |
| Iker Irisarri           | 0000-0002-3628-1137    |
| Tom Brown                | 0000-0001-8293-4816    |
| Solenne Correard         | 0000-0002-0554-5443    |
| Nefeli Kleopatra Venetsianou | 0009-0004-2472-2056 |
| Rafał Miłodrowski        | 0009-0002-4040-1231    |
| Nikita Kulikov           | 0000-0002-2566-6197    |
| Anestis Gkanogiannis     | 0000-0002-6441-0688    |
| Sebastian Martin         | 0000-0003-3171-4420    |
| Robert M. Waterhouse     | 0000-0003-4199-9052    |
| Thomas Crow              | 0009-0004-1195-0287    |
| Joy (Uditi) Shah         | —                      |
| Jia Zhang                | 0000-0003-1161-4836    |
| Cleopatra Petrohilos     | 0000-0002-8675-2726    |
| Julian Chiu              | 0009-0008-2264-0056    |
| Gareth Price             | 0000-0003-2439-8650    |
| George He                | 0009-0002-7417-8998    |

## License

Shield: [![CC BY 4.0][cc-by-shield]][cc-by]

This work is licensed under a
[Creative Commons Attribution 4.0 International License][cc-by].

[![CC BY 4.0][cc-by-image]][cc-by]

[cc-by]: http://creativecommons.org/licenses/by/4.0/
[cc-by-image]: https://i.creativecommons.org/l/by/4.0/88x31.png
[cc-by-shield]: https://img.shields.io/badge/License-CC%20BY%204.0-lightgrey.svg

