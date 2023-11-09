#!/usr/bin/env python3

import sys
from Pegasus.api import *
import os
from pathlib import Path
import logging

CREDS_FILE = Path("~/.pegasus/credentials.conf").expanduser()
osn_bucket = "BUCKET"

access_user = "USER"


def _main():
    year = 2010
    avg_type = "monthly"
    param_names = ["aspect", "elevation", "hillshading", "slope"]
    param_paths = [
        "s3://"
        + access_user
        + "@osn/"
        + osn_bucket
        + "/TerrainParameters/OK_10m/"
        + param
        + ".tif"
        for param in param_names
    ]
    shp_path = "s3://" + access_user + "@osn/" + osn_bucket + "/shpFiles/OK.zip"
    n_tiles = 6

    BASE_DIR = Path(".").resolve()

    stg_out = False  # Set to True to disable cleanup jobs on intermediate files

    # --- Properties ---------------------------------------------------------
    props = Properties()
    props["pegasus.monitord.encoding"] = "json"
    # props["pegasus.mode"] = "tutorial" # speeds up tutorial workflows - remove for production ones
    props[
        "pegasus.catalog.workflow.amqp.url"
    ] = "amqp://friend:donatedata@msgs.pegasus.isi.edu:5672/prod/workflows"
    props["pegasus.data.configuration"] = "nonsharedfs"
    props["pegasus.transfer.threads"] = "10"
    props["pegasus.transfer.lite.threads"] = "10"
    props["pegasus.integrity.checking"] = "none"  # temporary, bug
    # props["pegasus.transfer.bypass.input.staging"] = "true"
    props.write()  # written to ./pegasus.properties

    # --- RC ------------------------------------------------------------------
    rc = ReplicaCatalog()

    param_files = []
    for param_path in param_paths:
        param_files.append(File(os.path.basename(param_path)))
        rc.add_replica(site="osn", lfn=param_files[-1], pfn=param_path)

    shp_file = File(os.path.basename(shp_path))
    rc.add_replica(site="osn", lfn=shp_file, pfn=shp_path)

    rc.write()

    # --- Container ----------------------------------------------------------
    base_container = Container(
        "base-container",
        Container.SINGULARITY,
        image="docker://olayap/somospie-gdal-netcdf",
    )

    # --- Transformations -----------------------------------------------------
    get_sm = Transformation(
        "get_sm.py",
        site="local",
        pfn=Path(".").resolve() / "code/get_sm.py",
        is_stageable=True,
        container=base_container,
        arch=Arch.X86_64,
        os_type=OS.LINUX,
    ).add_profiles(Namespace.CONDOR, request_memory="3GB")

    generate_train = Transformation(
        "generate_train.py",
        site="local",
        pfn=Path(".").resolve() / "code/generate_train.py",
        is_stageable=True,
        container=base_container,
        arch=Arch.X86_64,
        os_type=OS.LINUX,
    ).add_profiles(Namespace.CONDOR, request_memory="3GB")

    generate_eval = Transformation(
        "generate_eval.py",
        site="local",
        pfn=Path(".").resolve() / "code/generate_eval.py",
        is_stageable=True,
        container=base_container,
        arch=Arch.X86_64,
        os_type=OS.LINUX,
    ).add_profiles(Namespace.CONDOR, request_memory="8GB", request_disk="70GB")

    tc = (
        TransformationCatalog()
        .add_containers(base_container)
        .add_transformations(get_sm, generate_train, generate_eval)
        .write()
    )  # written to ./transformations.yml

    # --- Site Catalog -------------------------------------------------
    osn = Site("osn", arch=Arch.X86_64, os_type=OS.LINUX)

    # create and add a bucket in OSN to use for your workflows
    osn_shared_scratch_dir = Directory(
        Directory.SHARED_SCRATCH, path="/" + osn_bucket + "/DataTransformation/work"
    ).add_file_servers(
        FileServer(
            "s3://" + access_user + "@osn/" + osn_bucket + "/DataTransformation/work",
            Operation.ALL,
        ),
    )
    osn_shared_storage_dir = Directory(
        Directory.SHARED_STORAGE, path="/" + osn_bucket + "/DataTransformation/storage"
    ).add_file_servers(
        FileServer(
            "s3://"
            + access_user
            + "@osn/"
            + osn_bucket
            + "/DataTransformation/storage",
            Operation.ALL,
        ),
    )
    osn.add_directories(osn_shared_scratch_dir, osn_shared_storage_dir)

    # add a local site with an optional job env file to use for compute jobs
    shared_scratch_dir = "{}/work".format(BASE_DIR)
    local_storage_dir = "{}/storage".format(BASE_DIR)
    local = Site("local").add_directories(
        Directory(Directory.SHARED_SCRATCH, shared_scratch_dir).add_file_servers(
            FileServer("file://" + shared_scratch_dir, Operation.ALL)
        ),
        Directory(Directory.LOCAL_STORAGE, local_storage_dir).add_file_servers(
            FileServer("file://" + local_storage_dir, Operation.ALL)
        ),
    )

    # job_env_file = Path(str(BASE_DIR) + "/../tools/job-env-setup.sh").resolve()
    # local.add_pegasus_profile(pegasus_lite_env_source=job_env_file)

    # condorpool_site = Site("condorpool")
    # condorpool_site.add_condor_profile(request_cpus=1, request_memory="9 GB", request_disk="9 GB")

    sc = SiteCatalog().add_sites(osn, local).write()  # written to ./sites.yml

    # --- Workflow ------------------------------------------------------------
    wf = Workflow("DataTransformation")

    avg_files = [File("{0:02d}.tif".format(month)) for month in range(1, 13)]

    job_get_sm = (
        Job(get_sm)
        .add_args("-y", year, "-a", avg_type, "-o", *avg_files)
        .add_outputs(*avg_files, stage_out=stg_out)
    )  # bypass_staging=False

    wf.add_jobs(job_get_sm)

    # Generate training files
    for i, avg_file in enumerate(avg_files):
        train_file = File("{0:04d}_{1:02d}.tif".format(year, i + 1))
        train_file_aux = File("{0:04d}_{1:02d}.tif.aux.xml".format(year, i + 1))
        job_generate_train = (
            Job(generate_train)
            .add_args(
                "-i",
                avg_file,
                "-o",
                train_file,
                "-f",
                *param_files,
                "-p",
                *param_names,
                "-s",
                shp_file
            )
            .add_inputs(avg_file, *param_files, shp_file)
            .add_outputs(train_file, train_file_aux, stage_out=True)
        )
        wf.add_jobs(job_generate_train)

    # Generate eval files
    if n_tiles == 0:
        eval_file = File("eval.tif")
        eval_file_aux = File("eval.tif.aux.xml")
        job_generate_eval = (
            Job(generate_eval)
            .add_args(
                "-i",
                *param_files,
                "-p",
                *param_names,
                "-n",
                n_tiles,
                "-s",
                shp_file,
                "-o",
                eval_file
            )
            .add_inputs(*param_files, shp_file)
            .add_outputs(eval_file, eval_file_aux, stage_out=True)
        )
        wf.add_jobs(job_generate_eval)

    tile_count = 0
    for i in range(n_tiles):
        for j in range(n_tiles):
            eval_path = "eval_{0:04d}.tif".format(tile_count)
            eval_file = File(eval_path)
            eval_file_aux = File(eval_path + ".aux.xml")
            job_generate_eval = (
                Job(generate_eval)
                .add_args(
                    "-i",
                    *param_files,
                    "-p",
                    *param_names,
                    "-n",
                    n_tiles,
                    "-x",
                    i,
                    "-y",
                    j,
                    "-s",
                    shp_file,
                    "-o",
                    eval_file
                )
                .add_inputs(*param_files, shp_file)
                .add_outputs(eval_file, eval_file_aux, stage_out=True)
            )
            wf.add_jobs(job_generate_eval)

            tile_count += 1

    try:
        wf.write()
        wf.write().graph(include_files=True, label="xform-id", output="graph.png")
    except PegasusClientError as e:
        print(e)


def main():
    logging.basicConfig(level=logging.DEBUG)
    if not CREDS_FILE.exists():
        logging.warn("OSN credentials files does not exists")
        # sys.exit(1)

    _main()


if __name__ == "__main__":
    main()
