package org.apache.spark.mllib.optimization

import breeze.optimize.{OWLQN => BreezeOWLQN, CachedDiffFunction}
import breeze.linalg.{DenseVector => BDV}

import org.apache.spark.Logging
import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.mllib.linalg.{Vectors, Vector}
import org.apache.spark.rdd.RDD

import scala.collection.mutable.ArrayBuffer

/**
 *  :: Developer Api ::
 *  Class used to solve an optimization problem with both L1 and L2 regularizations.
 *  Spark is used to compute and aggregate statistics needed to do OWL-QN steps
 *  The OWL-QN class from the breeze library orthant-projections and stepping.
 *  Reference: [[http://machinelearning.wustl.edu/mlpapers/paper_files/icml2007_AndrewG07.pdf]]
 *  @param gradient Gradient function to be used
 */
@DeveloperApi
class OWLQN(gradient: Gradient) extends LBFGS(gradient, new SquaredL2Updater) {
  // This has to be between 0 and 1
  // 1.0 == L1 regularization, 0.0 == L2 regularization
  private var alpha = 0.0

  def setAlpha(alpha: Double): this.type = {
    this.alpha = alpha
    this
  }

  override def optimize(data: RDD[(Double, Vector)], initialWeights: Vector): Vector = {
    val (weights, _) = OWLQN.runOWLQN(
      data,
      gradient,
      updater,
      numCorrections,
      convergenceTol,
      maxNumIterations,
      regParam,
      alpha,
      initialWeights)
    weights
  }
}

/**
* :: DeveloperApi ::
* Top-level method to run OWLQN.
*/
@DeveloperApi
object OWLQN extends Logging {
   /**
   * Run OWL-QN in parallel using mini batches.
   * The cost function to be used here is exactly the same as L-BFGS (which can handle L2 regularization as well).
   * Then only difference is that instead of L-BFGS from breeze, we use OWL-QN from breeze and we allow the user to
   * specify the alpha that determines regularization weights between L1 and L2.
   *
   * @param data - Input data for OWLQN. RDD of the set of data examples, each of the form (label, [feature values]).
   * @param gradient - Gradient object (used to compute the gradient of the loss function of
   *                   one single data example).
   * @param updater -Updater function to actually perform a gradient step in a given direction.
   * @param numCorrections - The number of corrections used in the OWLQN update.
   * @param maxNumIterations - Maximal number of iterations that OWLQN can be run.
   * @param regParam - Regularization parameter
   * @param alpha - Between 0.0 and 1.0. L1 weight becomes alpha * regParam. L2 weight becomes (1-alpha)*regParam
   * @param initialWeights - Initial weights to start the optimization process from.
   *
   * @return A tuple containing two elements. The first element is a column matrix containing
   *         weights for every feature, and the second element is an array containing the loss
   *         computed for every iteration.
   */
    def runOWLQN(
       data: RDD[(Double, Vector)],
       gradient: Gradient,
       updater: Updater,
       numCorrections: Int,
       convergenceTol: Double,
       maxNumIterations: Int,
       regParam: Double,
       alpha: Double,
       initialWeights: Vector): (Vector, Array[Double]) = {

      val lossHistory = new ArrayBuffer[Double](maxNumIterations)
      val numExamples = data.count()

      val l1RegParam = alpha * regParam
      val l2Regparam = (1.0 - alpha) * regParam

      //Cost function doesn't change from LBFGS because breeze's OWLQN code handles all the L1 related things.
      val costFun =
        new LBFGS.CostFun(data, gradient, updater, l2Regparam, numExamples)

      val owlqn = new BreezeOWLQN[BDV[Double]](maxNumIterations, numCorrections, l1RegParam, convergenceTol)

      val states =
        owlqn.iterations(new CachedDiffFunction(costFun), initialWeights.toBreeze.toDenseVector)

      var state = states.next()
      while (states.hasNext) {
        lossHistory.append(state.adjustedValue)
        state = states.next()
      }

      lossHistory.append(state.adjustedValue)
      val weights = Vectors.fromBreeze(state.x)


      logInfo("OWLQN.runMiniBatchOWLQN finished. Last 10 losses %s".format(
        lossHistory.takeRight(10).mkString(", ")))

      (weights, lossHistory.toArray)
    }
}

