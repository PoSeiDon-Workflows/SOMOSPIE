#!/usr/bin/env python3

import os
import logging
import calendar
from pathlib import Path
from Pegasus.api import *
from argparse import ArgumentParser

class DataTransformationWorkflow:
    wf = None
    sc = None
    tc = None
    rc = None
    props = None

    daxfile = None
    wf_name = None
    wf_dir = None
    
    #i'm still not sure what the n_tiles means exactly
    n_tiles = 2
    #shape file
    data_shp_zip = "LA_County_Boundary.zip"
    #file with links to the tiles that will be processed
    data_tiles_to_process = "LA_10m.txt"
    #file describing the projection to be used, NAD_83.wkt contains the North America specific conus representation
    #WGS_84.wkt contains the global one for all representation, less precise but generic
    data_projection_conf = "NAD_83.wkt"


    # --- Init ---------------------------------------------------------------------
    def __init__(self, year, daxfile="workflow.yml", singularity=False):
        self.daxfile = daxfile
        self.year = year
        self.singularity = singularity
        self.wf_dir = Path(__file__).parent.resolve()
        self.version = "v07.1"
        self.wf_name = f"somospie-data-wf-{self.year}"
        
        # Read file with links
        self.input_tiles = []
        self.input_tiles_pfns = []
        with open(os.path.join(self.wf_dir, "config", self.data_tiles_to_process), 'r') as f:
            download_links = f.read().splitlines()
            for f in download_links:
                self.input_tiles.append(File(os.path.basename(f.strip())))
                self.input_tiles_pfns.append(f)
        
        self.soil_moisture_inputs = []
        self.soil_moisture_inputs_pfns = []
        for month in range(1, 13):
            for day in range(1, calendar.monthrange(year, month)[1] + 1):
                file_pfn = f"https://dap.ceda.ac.uk/neodc/esacci/soil_moisture/data/daily_files/COMBINED/{self.version}/2010/ESACCI-SOILMOISTURE-L3S-SSMV-COMBINED-{self.year}{month:02d}{day:02d}000000-fv07.1.nc"
                file_lfn = f"{self.year}_{month:02d}_{day:02d}.nc"
                self.soil_moisture_inputs.append(File(file_lfn))
                self.soil_moisture_inputs_pfns.append(file_pfn)

    # --- Write files in directory -------------------------------------------------
    def write(self):
        self.sc.write()
        self.rc.write()
        self.tc.write()

        self.props.write()
        with open(self.daxfile, "w+") as f:
            self.wf.write(f)


    # --- Configuration (Pegasus Properties) ---------------------------------------
    def create_pegasus_properties(self):
        self.props = Properties()

        self.props["pegasus.transfer.bypass.input.staging"] = "true"
        self.props["pegasus.transfer.threads"] = "10"

        self.props["pegasus.monitord.encoding"] = "json"
        self.props["pegasus.catalog.workflow.amqp.url"] = "amqp://friend:donatedata@msgs.pegasus.isi.edu:5672/prod/workflows"


    # --- Site Catalog -------------------------------------------------------------
    def create_sites_catalog(self):
        self.sc = SiteCatalog()

        shared_scratch_dir = os.path.join(self.wf_dir, "scratch")
        local_storage_dir = os.path.join(self.wf_dir, "output")

        local = Site("local")\
                    .add_directories(
                        Directory(Directory.SHARED_SCRATCH, shared_scratch_dir)
                            .add_file_servers(FileServer("file://" + shared_scratch_dir, Operation.ALL)),

                        Directory(Directory.LOCAL_STORAGE, local_storage_dir)
                            .add_file_servers(FileServer("file://" + local_storage_dir, Operation.ALL))
                    )

        condorpool = Site("condorpool")\
                        .add_condor_profile(universe="vanilla")\
                        .add_pegasus_profile(
                            style="condor",
                            data_configuration="condorio"
                        )

        self.sc.add_sites(local, condorpool)


    # --- Transformation Catalog (Executables and Containers) ----------------------
    def create_transformation_catalog(self):
        self.tc = TransformationCatalog()

        base_container = Container("base-container",
            container_type = Container.SINGULARITY if self.singularity else Container.DOCKER,
            image="docker://olayap/somospie-gdal-netcdf:latest",
            image_site="docker_hub"
        )

        # --- Transformations ----------------------------------------------------------
        merge = Transformation(
                "merge",
                site="local",
                pfn=os.path.join(self.wf_dir, "code/merge.py"),
                is_stageable=True,
                container=base_container
            )

        reproject = Transformation(
                "reproject",
                site="local",
                pfn=os.path.join(self.wf_dir, "code/reproject.py"),
                is_stageable=True,
                container=base_container
            )

        crop = Transformation(
                "crop",
                site="local",
                pfn=os.path.join(self.wf_dir, "code/crop.py"),
                is_stageable=True,
                container=base_container
            )

        compute = Transformation(
                "compute",
                site="local",
                pfn=os.path.join(self.wf_dir, "code/compute.py"),
                is_stageable=True,
                container=base_container
            )

        merge_avg = Transformation(
                "merge_avg",
                site="local",
                pfn=os.path.join(self.wf_dir, "code/merge_avg.py"),
                is_stageable=True,
                container=base_container
            )

        get_sm = Transformation(
                "get_sm",
                site="local",
                pfn=os.path.join(self.wf_dir, "code/get_sm.py"),
                is_stageable=True,
                container=base_container
            )

        generate_train = Transformation(
                "generate_train",
                site="local",
                pfn=os.path.join(self.wf_dir, "code/generate_train.py"),
                is_stageable=True,
                container=base_container
            )

        generate_eval = Transformation(
                "generate_eval",
                site="local",
                pfn=os.path.join(self.wf_dir, "code/generate_eval.py"),
                is_stageable=True,
                container=base_container
            )

        self.tc.add_containers(base_container)
        self.tc.add_transformations(merge, reproject, crop, compute, merge_avg, get_sm, generate_train, generate_eval)

     # --- Replica Catalog ----------------------------------------------------------
    def create_replica_catalog(self):
        self.rc = ReplicaCatalog()
        
        self.rc.add_replica(site="local", lfn=self.data_projection_conf, pfn=os.path.join(self.wf_dir, "config", self.data_projection_conf))
        self.rc.add_replica(site="local", lfn=self.data_shp_zip, pfn=os.path.join(self.wf_dir, "config", self.data_shp_zip))

        for i in range(len(self.input_tiles)):
            self.rc.add_replica("AmazonS3", self.input_tiles[i], self.input_tiles_pfns[i])
        
        for i in range(len(self.soil_moisture_inputs)):
            self.rc.add_replica("ceda.ac.uk", self.soil_moisture_inputs[i], self.soil_moisture_inputs_pfns[i])


    # --- Workflow -------------------------------------------------------------------
    def create_workflow(self):
        self.wf = Workflow(self.wf_name, infer_dependencies=True)

        #### GeoTiled Workflow Part ####

        mosaic = File("mosaic.tif")
        job_merge = Job("merge")\
                .add_args("-i", *self.input_tiles, "-o", mosaic)\
                .add_inputs(*self.input_tiles, bypass_staging=True)\
                .add_outputs(mosaic, stage_out=True)

        self.wf.add_jobs(job_merge)

        dem_m = File("elevation_m.tif")
        projection = File(self.data_projection_conf)
        job_reproject = Job("reproject")\
                    .add_args("-p", projection,"-i", mosaic, "-o", dem_m)\
                    .add_inputs(mosaic, projection)\
                    .add_outputs(dem_m, stage_out=True)

        self.wf.add_jobs(job_reproject)

        # Crop tiles and compute each parameter for each tile
        aspect_tiles = []
        hillshading_tiles = []
        slope_tiles = []

        tile_count = 0
        for i in range(self.n_tiles):
            for j in range(self.n_tiles):
                tile = File("tile_{0:04d}.tif".format(tile_count))
                aspect_tiles.append(File("aspect_tile_{0:04d}.tif".format(tile_count)))
                hillshading_tiles.append(File("hillshading_tile_{0:04d}.tif".format(tile_count)))
                slope_tiles.append(File("slope_tile_{0:04d}.tif".format(tile_count)))

                job_crop = Job("crop")\
                            .add_args("-n", self.n_tiles, "-x", i, "-y", j, "-i", dem_m, "-o", tile)\
                            .add_inputs(dem_m)\
                            .add_outputs(tile, stage_out=True)

                job_compute = Job("compute")\
                            .add_args("-i", tile, "-o", aspect_tiles[-1], hillshading_tiles[-1], slope_tiles[-1])\
                            .add_inputs(tile)\
                            .add_outputs(aspect_tiles[-1], hillshading_tiles[-1], slope_tiles[-1], stage_out=True)

                tile_count += 1
                self.wf.add_jobs(job_crop, job_compute)

        # DEM reprojected to WGS84
        dem = File("elevation.tif")
        job_reproject = Job("reproject")\
                    .add_args("-p", 'EPSG:4326',"-i", mosaic, "-o", dem, "-n", "y")\
                    .add_inputs(mosaic)\
                    .add_outputs(dem, stage_out=True)

        # Mosaic of each parameter
        aspect = File("aspect.tif")
        job_avg0 = Job("merge_avg")\
                .add_args("-i", *aspect_tiles, "-o", aspect)\
                .add_inputs(*aspect_tiles)\
                .add_outputs(aspect, stage_out=True)

        hillshading = File("hillshading.tif")
        job_avg1 = Job("merge_avg")\
                .add_args("-i", *hillshading_tiles, "-o", hillshading)\
                .add_inputs(*hillshading_tiles)\
                .add_outputs(hillshading, stage_out=True)

        slope = File("slope.tif")
        job_avg2 = Job("merge_avg")\
                .add_args("-i", *slope_tiles, "-o", slope)\
                .add_inputs(*slope_tiles)\
                .add_outputs(slope, stage_out=True)

        self.wf.add_jobs(job_reproject, job_avg0, job_avg1, job_avg2)
        
        #### ML Data Preparation Workflow Part ####

        param_files = [dem, aspect, hillshading, slope]
        param_names = ["aspect", "elevation", "hillshading", "slope"]
        #avg_files = [File('{0:02d}.tif'.format(month)) for month in range(1, 13)]
        avg_files = []
        for month in range(1, 13):
            avg_file = File(f"{month:02d}.tif")
            avg_files.append(avg_file)
            soil_moisture_inputs_month = [fin for fin in self.soil_moisture_inputs if fin.lfn.startswith(f"{self.year}_{month:02d}_")]

            job_get_sm = Job("get_sm")\
                .add_args("-y", self.year, "-m", month, "-o", avg_file)\
                .add_inputs(*self.soil_moisture_inputs_month)\
                .add_outputs(avg_file, stage_out=True)

            self.wf.add_jobs(job_get_sm)

        shp_file = File(self.data_shp_zip)
        # Generate training files
        for i, avg_file in enumerate(avg_files):
            train_file = File('{0:04d}_{1:02d}.tif'.format(self.year, i + 1))
            train_file_aux = File('{0:04d}_{1:02d}.tif.aux.xml'.format(self.year, i + 1))
            job_generate_train = Job("generate_train")\
                        .add_args("-i", avg_file, "-o", train_file, "-f", *param_files, "-p", *param_names, "-s", shp_file)\
                        .add_inputs(avg_file, *param_files, shp_file)\
                        .add_outputs(train_file, train_file_aux, stage_out=True)
            self.wf.add_jobs(job_generate_train)

        # Generate eval files
        if self.n_tiles == 0:
            eval_file = File('eval.tif')
            eval_file_aux = File("eval.tif.aux.xml")
            job_generate_eval = Job("generate_eval")\
                    .add_args("-i", *param_files, "-p", *param_names, "-n", self.n_tiles, "-s", shp_file, "-o", eval_file)\
                    .add_inputs(*param_files, shp_file)\
                    .add_outputs(eval_file, eval_file_aux, stage_out=True)
            self.wf.add_jobs(job_generate_eval)

        tile_count = 0
        for i in range(self.n_tiles):
            for j in range(self.n_tiles):
                eval_path = "eval_{0:04d}.tif".format(tile_count)
                eval_file = File(eval_path)
                eval_file_aux = File(eval_path + ".aux.xml")
                job_generate_eval = Job("generate_eval")\
                        .add_args("-i", *param_files, "-p", *param_names, "-n", self.n_tiles, "-x", i, "-y", j, "-s", shp_file, "-o", eval_file)\
                        .add_inputs(*param_files, shp_file)\
                        .add_outputs(eval_file, eval_file_aux, stage_out=True)
                self.wf.add_jobs(job_generate_eval)

                tile_count += 1


    # --- Plan -----------------------------------------------------------------------
    def plan_workflow(self, submit=False):
        try:
            self.wf.plan(sites=["condorpool"], output_dir="output", dir="submit", output_sites=["local"], submit=submit)
            if submit:
                self.wf.wait()
        except PegasusClientError as e:
            print(e)
    

def main():
    parser = ArgumentParser(description="SOMOSPIE Pegasus Generator")
    parser.add_argument("--year", metavar="INT", type=int, default=2010, choices=range(1970,2023), help="Year To Pull Data For (Default: 2010)", required=False)
    parser.add_argument("--plan", action="store_true", help="Plan the Workflow", required=False)
    parser.add_argument("--submit", action="store_true", help="Plan and Submit the Workflow", required=False)
    parser.add_argument("--singularity", action="store_true", help="Use Singularity Instead of Docker", required=False)
    parser.add_argument("--output", metavar="STR", type=str, default="workflow.yml", help="Output file", required=False)

    args = parser.parse_args()

    workflow = DataTransformationWorkflow(year=args.year, daxfile=args.output, singularity=args.singularity)

    workflow.create_pegasus_properties()
    workflow.create_sites_catalog()
    workflow.create_transformation_catalog()
    workflow.create_replica_catalog()
    workflow.create_workflow()

    workflow.write()

    if args.plan or args.submit:
        workflow.plan_workflow(args.submit)


if __name__ == "__main__":
    main()
