/**
 * Spark access to data residing in an HPCC environment.
 * The JAPI class library from HPCCSystems is used to used to access
 * the wsECL services to obtain the location and layout of the file.  An
 * RDD is provided to read the file in parallel by file part.
 * @author holtjd
 *
 */
package org.hpccsystems.spark;